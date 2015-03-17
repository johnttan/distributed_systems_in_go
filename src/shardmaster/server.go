package shardmaster

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

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	configs  []Config // indexed by config num
	cache    map[int64]string
	requests map[int64]int64
	data     map[string]string

	//latest seq applied to data.
	latestSeq int
}

type Op struct {
	Key      string
	Value    string
	Op       string
	ReqID    int64
	ClientID int64
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.

	return nil
}

func (sm *ShardMaster) TryUntilAccepted(newOp Op) {
	// Keep trying new sequence slots until successfully committed to log.
	seq := sm.px.Max() + 1
	sm.px.Start(seq, newOp)
	to := 5 * time.Millisecond
	for {
		status, untypedOp := sm.px.Status(seq)
		if status {
			op := untypedOp.(Op)
			if op.ReqID == newOp.ReqID && op.ClientID == newOp.ClientID {
				return
			} else {
				seq += 1
				// seq = sm.px.Max() + 1
				sm.px.Start(seq, newOp)
			}
		}
		time.Sleep(to)
		if to < 100*time.Millisecond {
			to *= 2
		}
	}
}

func (sm *ShardMaster) CommitAll(op Op) string {
	var finalResults string
	for i := sm.latestSeq + 1; i <= sm.px.Max(); i++ {
		success, untypedOp := sm.px.Status(i)
		noOp := Op{}
		sm.px.Start(i, noOp)
		// Retry noOps until log is filled at current position
		for !success {
			time.Sleep(20 * time.Millisecond)
			success, untypedOp = sm.px.Status(i)
		}
		newOp := untypedOp.(Op)
		if id, okreq := sm.requests[newOp.ClientID]; !okreq || newOp.ReqID > id {
			result := sm.Commit(newOp)
			sm.requests[newOp.ClientID] = newOp.ReqID
			sm.cache[newOp.ClientID] = result
			if newOp.ClientID == op.ClientID && newOp.ReqID == op.ReqID {
				finalResults = result
			}
		} else {
			finalResults = sm.cache[newOp.ClientID]
		}
		sm.latestSeq = i
		sm.px.Done(i)
	}
	return finalResults
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
	sm.dead = true
	sm.l.Close()
	sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	gob.Register(Op{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	sm.requests = make(map[int64]int64)
	sm.cache = make(map[int64]string)
	sm.data = make(map[string]string)
	sm.latestSeq = -1

	rpcs := rpc.NewServer()
	rpcs.Register(sm)

	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.dead == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.dead == false {
				if sm.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.unreliable && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.dead == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
