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

func DPrintf(me int, format string, a ...interface{}) (n int, err error) {
	errOne := errors.New("err")
	if Debug > 0 {
		log.Println("", errOne)
		log.Printf("Inside server -> %v", me)
		log.Println()
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	Op       string
	ReqID    int64
	ClientID int64
	Config   shardmaster.Config
	UID int64
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
	cache    map[int64]string
	requests map[int64]int64
	data     map[string]string

	config shardmaster.Config

	//latest seq applied to data.
	latestSeq int
}

func (kv *ShardKV) validateOp(op Op) (string, Err) {
	switch op.Op {
		case "Config":
			if op.Config.Num >= kv.config.Num {
				return "", OK
			}
		case "Get", "Put", "Append":
			shard := key2shard(op.Key)
			if kv.gid != kv.config.Shards[shard] {
				return "", ErrWrongGroup
			}

			if op.ReqID < kv.requests[op.ClientID] {
				return kv.cache[op.ClientID], OK
			}
	}
	return "", ""
}

func (kv *ShardKV) commit (op Op) {
	var returnValue string
	switch op.Op {
	case "Get":
		returnValue = kv.data[op.Key]
	case "Put":
		kv.data[op.Key] = op.Value
	case "Append":
		returnValue = kv.data[op.Key]
		kv.data[op.Key] = kv.data[op.Key] + op.Value
	}

	// Cache return values and latest ReqID
	kv.requests[op.ClientID] = op.ReqID
	kv.cache[op.ClientID] = value
}

func (kv *ShardKV) logOp (newOp Op) (string, Err) {
	// Keep trying new sequence slots until successfully committed to log.
	seq := kv.latestSeq + 1
	kv.px.Start(seq, newOp)
	to := 100 * time.Millisecond
	for {
		status, untypedOp := kv.px.Status(seq)
		if status {
			op := untypedOp.(Op)
			// Check if operation has been cached or is invalid because of wrong group
			_, err := kv.validateOp(op)
			// This validation is to see if op has been invalidated by previous ops
			value, oldE := kv.validateOp(newOp)

			// If op we are trying to commit is not valid, return value,err
			if oldE != "" {
				return value, err
			}

			// If the current op from log is valid, then commit it
			if err == "" {
				kv.commit(op)
				kv.px.Done(seq)
				kv.latestSeq = seq
			}
			if op.UID == newOp.UID {
				// Return the cached version, and OK
				return kv.cache[op.ClientID], OK
			} else {
				seq += 1
				kv.px.Start(seq, newOp)
			}
		}
		time.Sleep(to)
	}
}

func (kv *ShardKV) tryOp(op Op) (string, Err) {
	op.UID = nrand()
	value, err := kv.logOp(op)
	return value, err
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	getOp := Op{
		Op:       "Get",
		Key:      args.Key,
		ReqID:    args.ReqID,
		ClientID: args.ClientID,
		Config:   args.Config,
	}

	reply.Value, reply.Err := kv.tryOp(getOp)

	return nil
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

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
