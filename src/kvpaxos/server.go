package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
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
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	cache    map[int64]string
	requests map[int64]int64
	data     map[string]string

	//latest seq applied to data.
	latestSeq int
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	id, okreq := kv.requests[args.ClientID]
	if !okreq || args.ReqID > id {
		newOp := Op{args.Key, "", "Get", args.ReqID, args.ClientID}
		kv.TryUntilAccepted(newOp)
		kv.CommitAll(newOp)
	}
	reply.Value = kv.cache[args.ClientID]
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	id, okreq := kv.requests[args.ClientID]
	if !okreq || args.ReqID > id {
		newOp := Op{args.Key, args.Value, args.Op, args.ReqID, args.ClientID}
		kv.TryUntilAccepted(newOp)
		kv.CommitAll(newOp)
	}
	reply.PreviousValue = kv.cache[args.ClientID]
	return nil
}

func (kv *KVPaxos) TryUntilAccepted(newOp Op) {
	// Keep trying new sequence slots until successfully committed to log.
	seq := kv.px.Max() + 1
	kv.px.Start(seq, newOp)
	to := 5 * time.Millisecond
	for {
		status, untypedOp := kv.px.Status(seq)
		if status {
			op := untypedOp.(Op)
			if op.ReqID == newOp.ReqID && op.ClientID == newOp.ClientID {
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

func (kv *KVPaxos) CommitAll(op Op) string {
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
		if id, okreq := kv.requests[newOp.ClientID]; !okreq || newOp.ReqID > id {
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

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
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
				if kv.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.unreliable && (rand.Int63()%1000) < 200 {
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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
