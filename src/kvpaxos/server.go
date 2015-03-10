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

const Debug = 0

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
	Key            string
	Value          string
	Op             string
	ReqID          int64
	ClientID       int64
	GetReply       *GetReply
	PutAppendReply *PutAppendReply
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	cache       map[int64]Op
	requests    map[int64]int64
	data        map[string]string
	start_chan  chan int
	requestLock sync.Mutex
	//latest seq applied to data.
	latestSeq int
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.NotDone(args.ClientID, args.ReqID) {
		newOp := Op{Key: args.Key, Value: "", Op: "Get", ReqID: args.ReqID, ClientID: args.ClientID}
		i := kv.TryUntilAccepted(newOp)
		// DPrintf("SENDING TO CHAN, val %v", i)
		kv.start_chan <- i
	}

	// DPrintf("NOT DOING GET, %+v", args)
	for kv.NotDone(args.ClientID, args.ReqID) {
		time.Sleep(20 * time.Millisecond)
	}

	kv.requestLock.Lock()
	defer kv.requestLock.Unlock()
	// Old request, don't need to return anything. Client has moved on
	if kv.cache[args.ClientID].ReqID > args.ReqID {
		return nil
	}
	DPrintf("CACHED FOR GET IS %+v, currentArgs %+v", kv.cache[args.ClientID].GetReply, args)
	reply.Value = kv.cache[args.ClientID].GetReply.Value
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.Value == "15" {
		DPrintf("GOT APPEND, args=%+v, reqs=%+v", args, kv.requests)
	}
	if kv.NotDone(args.ClientID, args.ReqID) {
		newOp := Op{Key: args.Key, Value: args.Value, Op: args.Op, ReqID: args.ReqID, ClientID: args.ClientID}
		if args.Value == "15" {
			DPrintf("TRYING THIS APPEND, newOp=%+v", newOp)
		}
		i := kv.TryUntilAccepted(newOp)
		DPrintf("SENDING TO CHAN, val %v, op: %+v", i, newOp)
		kv.start_chan <- i
	}

	DPrintf("NOT DOING PUTAPPEND %+v", args)
	for kv.NotDone(args.ClientID, args.ReqID) {
		time.Sleep(20 * time.Millisecond)
	}
	DPrintf("DONE WITH PUTAPPEND %+v", kv.cache[args.ClientID])
	// Old request, don't need to return anything. Client has moved on
	kv.requestLock.Lock()
	defer kv.requestLock.Unlock()
	if kv.cache[args.ClientID].ReqID > args.ReqID {
		return nil
	}
	// DPrintf("CACHED FOR PUTAPPEND IS %+v, currentArgs %+v", kv.cache[args.ClientID].PutAppendReply, args)
	reply.PreviousValue = kv.cache[args.ClientID].PutAppendReply.PreviousValue
	return nil
}

func (kv *KVPaxos) TryUntilAccepted(newOp Op) int {
	// Keep trying new sequence slots until successfully committed to log.
	seq := kv.px.Max() + 1
	kv.px.Start(seq, newOp)
	to := 10 * time.Millisecond
	for {
		status, untypedOp := kv.px.Status(seq)
		// DPrintf("TRYING THIS OP=%+v , in seq=%v", newOp, seq)
		if status {
			// DPrintf("GOT OP ACCEPTED %+v, current newOp = %+v, seq = %v, min = %v, requests = %+v", untypedOp, newOp, seq, kv.px.Min(), kv.requests)
			if untypedOp != nil {
				op := untypedOp.(Op)
				if op.ReqID == newOp.ReqID && op.ClientID == newOp.ClientID {
					// DPrintf("GOT THIS TO GO THROUGH %v", seq)
					return seq
				}
			}
			seq++

			kv.px.Start(seq, newOp)
		}
		time.Sleep(to)
		if to < 50*time.Millisecond {
			to *= 2
		}
	}
}

func (kv *KVPaxos) NotDone(clientid int64, reqid int64) bool {
	kv.requestLock.Lock()
	defer kv.requestLock.Unlock()
	id, okreq := kv.requests[clientid]
	notDone := !okreq || reqid > id
	return notDone
}

// func (kv *KVPaxos) CommitAll(op Op) {
// 	for i := kv.latestSeq + 1; i <= kv.px.Max(); i++ {
// 		success, untypedOp := kv.px.Status(i)
// 		noOp := Op{}
// 		kv.px.Start(i, noOp)
// 		// Retry noOps until log is filled at current position
// 		for !success {
// 			time.Sleep(20 * time.Millisecond)
// 			success, untypedOp = kv.px.Status(i)
// 		}
// 		newOp := untypedOp.(Op)
// 		if kv.NotDone(newOp.ClientID, newOp.ReqID) {
// 			result := kv.Commit(newOp)
// 			kv.requests[newOp.ClientID] = newOp.ReqID
// 			// DPrintf("COMMITTING, current OP IS  %+v, wanted OP IS %+v", newOp, op)
// 			kv.cache[newOp.ClientID] = result
// 		}
// 		kv.latestSeq = i
// 		kv.px.Done(i)
// 	}
// }

func (kv *KVPaxos) TryRepeatedly() {
	i := 0
	for {
		select {
		case wantToStart := <-kv.start_chan:
			// DPrintf("STARTING NOOP FOR %v wantoStart, i%v", wantToStart, i)
			for seq := i; seq < wantToStart; seq++ {
				noOp := Op{}
				kv.px.Start(seq, noOp)
			}
		default:
			success, untypedOp := kv.px.Status(i)
			// Retry noOps until log is filled at current position
			if success {
				newOp := untypedOp.(Op)
				if kv.NotDone(newOp.ClientID, newOp.ReqID) {
					result := kv.Commit(newOp)
					kv.requestLock.Lock()
					kv.requests[newOp.ClientID] = newOp.ReqID
					DPrintf("COMMITTING, current OP IS  %+v, result:%+v", newOp, result)
					kv.cache[newOp.ClientID] = result
					kv.requestLock.Unlock()
					kv.latestSeq = i
					kv.px.Done(i)
				}
				i++
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
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
	kv.cache = make(map[int64]Op)
	kv.data = make(map[string]string)
	kv.latestSeq = -1
	kv.start_chan = make(chan int)

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)
	go kv.TryRepeatedly()
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
