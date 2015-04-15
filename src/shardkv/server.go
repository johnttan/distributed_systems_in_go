package shardkv

import "net"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"

import "errors"

const Debug = 0

// TODO: Implement more effective handoff strategy such that G1 -> G2 handoff prevents G1 from handling requests after initiation of handoff
func DPrintf(me int64, format string, a ...interface{}) (n int, err error) {
	errOne := errors.New("err")
	if Debug > 0 {
		log.Println("", errOne)
		log.Printf("Inside server -> %v", me)
		log.Println()
		log.Printf(format, a...)
	}
	return
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	Unreliable bool // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	cache           map[int64]string
	requests        map[int64]int64
	data            map[string]string
	shardsOffline   []bool
	config          shardmaster.Config
	waitingOnShards []bool
	shardsNeeded map[int]bool
	newConfig       shardmaster.Config

	client *Clerk
	//latest seq applied to data.
	latestSeq int
}

func (kv *ShardKV) validateOp(op Op) (string, Err) {
	switch op.Op {
	case "noop":
		return "", ""
	case "StartConfig":
		// If config > op.Config, then we've already transitioned.
		// Return an OK error to bypass commit.
		if kv.config.Num >= op.Config.Num || kv.isConfiguring() {
			return "", ErrWrongGroup
		}
	case "ReceiveShard":
		// Already received shard

		if op.Config.Num > kv.config.Num {
			return "", ErrNotReady
		}
		if op.Config.Num == kv.config.Num && !kv.isConfiguring() {
			return "", ErrNotReady
		}
	case "Get", "Put", "Append":
		shard := key2shard(op.Key)
		if kv.isConfiguring() {
			return "", ErrWrongGroup
		}
		if op.ReqID <= kv.requests[op.ClientID] {
			return kv.cache[op.ClientID], OK
		}
		if kv.gid != kv.config.Shards[shard] {
			return "", ErrWrongGroup
		}

	}
	return "", ""
}

func (kv *ShardKV) commit(op Op) {
	var returnValue string
	switch op.Op {
	case "Get":
		returnValue = kv.data[op.Key]
		DPrintf(kv.gid, "committing get, op=%+v, data = %+v, config=%+v", op, kv.data, kv.config)
	case "Put":
		kv.data[op.Key] = op.Value
	case "Append":
		returnValue = kv.data[op.Key]
		kv.data[op.Key] = kv.data[op.Key] + op.Value
	case "StartConfig":
		kv.shardsNeeded = make(map[int]bool)
		for shard := 0; shard < NShards; shard++ {
			// If new shard, or old shard that is now unavailable, mark it offline
			if op.Config.Num > 1 {
				if kv.config.Shards[shard] == kv.gid && op.Config.Shards[shard] != kv.gid {
					data := make(map[string]string)
					for key, val := kv.data {
						if key2shard(key) == shard {
							data[key] = val
						}
					}
					// Send it off
					go kv.client.SendShard(op.Config.Groups[op.Config.Shards[shard]], op.Config, shard, data)
					kv.shardsNeeded[shard] = true
				}
			}
			kv.config = op.Config
		}
	case "StopConfig":
		kv.config = kv.newConfig
		kv.reconfiguring = false
		DPrintf(kv.gid, "ENDED CONFIGURATION %+v, data=%+v", kv.config, kv.data)
		for ind, _ := range kv.shardsOffline {
			kv.shardsOffline[ind] = false
		}
	case "SendShard":
		args := &RequestKVArgs{Shard: op.Shard}
		reply := &RequestKVReply{}
		kv.getShard(args, reply)
		DPrintf(kv.gid, "sending shard")

		reply.Shard = op.Shard
		reply.Config = op.Config
		gid := kv.newConfig.Shards[op.Shard]
		done := kv.SendShard(gid, reply)
		for !done {
			done = kv.SendShard(gid, reply)
		}
	case "ReceiveShard":
		DPrintf(kv.gid, "received shard %+v", op.MigrationReply)
		kv.merge(kv.requests, kv.cache, kv.data, op.MigrationReply)
		kv.shardsOffline[op.Shard] = false
		kv.shardsToReceive[op.Shard] = false
	}

	// Cache return values and latest ReqID
	kv.requests[op.ClientID] = op.ReqID
	kv.cache[op.ClientID] = returnValue
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	getOp := Op{
		Op:       "Get",
		Key:      args.Key,
		ReqID:    args.ReqID,
		ClientID: args.ClientID,
	}

	value, err := kv.tryOp(getOp)
	reply.Value = value
	reply.Err = err

	return nil
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	putAppendOp := Op{
		Op:       args.Op,
		Key:      args.Key,
		ReqID:    args.ReqID,
		ClientID: args.ClientID,
		Value:    args.Value,
	}

	value, err := kv.tryOp(putAppendOp)
	reply.PreviousValue = value
	reply.Err = err
	// DPrintf(kv.gid, "PUTAPPEND reply %+v, config=%+v, \n\nargs =%+v", reply, kv.config, args)
	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.tryOp(Op{Op: "noop"})
	newConfig := kv.sm.Query(kv.config.Num + 1)
	// If it is next config to process
	if newConfig.Num == kv.config.Num+1 {
		startOp := Op{Op: "StartConfig", Config: *config}
		kv.tryOp(startOp)
	}
}

// tell the server to shut itself down.
func (kv *ShardKV) kill() {
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
	kv.requests = make(map[int64]int64)
	kv.cache = make(map[int64]string)
	kv.data = make(map[string]string)
	kv.latestSeq = -1
	kv.config = shardmaster.Config{Num: -1}
	kv.shardsOffline = make([]bool, NShards)
	kv.waitingOnShards = make([]bool, NShards)
	kv.shardsNeeded = make(map[int]bool)
	kv.clerk = MakeClerk(shardmasters)

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.Unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.Unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						// fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.dead == false {
				// fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.dead == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
