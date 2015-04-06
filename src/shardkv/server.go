package shardkv

import "net"
import "fmt"
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

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
		log.Println()
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
	Config   Config
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

	config Config

	//latest seq applied to data.
	latestSeq int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	id, okreq := kv.requests[args.ClientID]
	if !okreq || args.ReqID > id {
		newOp := Op{args.Key, "", "Get", args.ReqID, args.ClientID, Config{}}
		kv.TryUntilAccepted(newOp)
		kv.CommitAll(newOp)
	}
	reply.Value = kv.cache[args.ClientID]
	reply.Err = OK
	return nil
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	id, okreq := kv.requests[args.ClientID]
	if !okreq || args.ReqID > id {
		newOp := Op{args.Key, args.Value, args.Op, args.ReqID, args.ClientID, Config{}}
		kv.TryUntilAccepted(newOp)
		kv.CommitAll(newOp)
	}
	reply.PreviousValue = kv.cache[args.ClientID]
	reply.Err = OK
	return nil
}

func (kv *ShardKV) TryUntilAccepted(newOp Op) {
	// Keep trying new sequence slots until successfully committed to log.
	// DPrintf("TRYING FOR %+v", newOp)
	seq := kv.px.Max() + 1
	kv.px.Start(seq, newOp)
	to := 5 * time.Millisecond
	for {
		status, untypedOp := kv.px.Status(seq)
		if status {
			op := untypedOp.(Op)
			if (op.ReqID == newOp.ReqID && op.ClientID == newOp.ClientID) || (newOp.Config.Num > 0 && newOp.Config.Num == op.Config.Num) {
				return
			} else {
				seq += 1
				// seq = kv.px.Max() + 1
				kv.px.Start(seq, newOp)
			}
		}
		time.Sleep(to)
		if to < 100*time.Millisecond {
			to *= 2
		}
	}
}

func (kv *ShardKV) CommitAll(op Op) string {
	var finalResults string
	for i := kv.latestSeq + 1; i <= kv.px.Max(); i++ {
		success, untypedOp := kv.px.Status(i)
		noOp := Op{}
		kv.px.Start(i, noOp)
		// Retry noOps until log is filled at current position
		for !success {
			time.Sleep(20 * time.Millisecond)
			success, untypedOp = kv.px.Status(i)
		}
		newOp := untypedOp.(Op)
		if id, okreq := kv.requests[newOp.ClientID]; !okreq || newOp.ReqID > id || newOp.Op == "Config" {
			result := kv.Commit(newOp)
			kv.requests[newOp.ClientID] = newOp.ReqID
			kv.cache[newOp.ClientID] = result
			if newOp.ClientID == op.ClientID && newOp.ReqID == op.ReqID {
				finalResults = result
			}
		} else {
			finalResults = kv.cache[newOp.ClientID]
		}
		kv.latestSeq = i
		kv.px.Done(i)
	}
	return finalResults
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	latestConfig := kv.sm.Query(-1)
	configNum := kv.config.Num
	if latestConfig.Num > configNum {
		kv.CommitAll(Op{})
	}
	latestConfig = kv.sm.Query(-1)
	configNum = kv.config.Num
	if latestConfig.Num > configNum {

		for configNum < latestConfig.Num {
			nextConfig := kv.sm.Query(configNum + 1)
			configOp := Op{Op: "Config", Config: Config(nextConfig)}
			kv.TryUntilAccepted(configOp)
			configNum++
		}
		kv.CommitAll(Op{})
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
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.dead == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
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
