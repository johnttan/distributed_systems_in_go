package shardmaster

import "net"
import "net/rpc"
import "log"
import crand "crypto/rand"
import "math/big"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"
import "fmt"
import "strings"

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf(format+"\n", a...)
	}
	return
}

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos
	peers      []string
	configs    []Config // indexed by config num
	previous   map[int64]bool
	requests   map[int64]interface{}
	//latest seq applied to data.
	latestSeq int
}

type Op struct {
	Op      string
	ID      int64
	GID     int64
	Servers []string
	Shard   int
	Num     int
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	newOp := Op{Op: "Join", GID: args.GID, Servers: args.Servers}
	sm.TryUntilAccepted(&newOp)
	sm.CommitAll(newOp)
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	newOp := Op{Op: "Leave", GID: args.GID}
	sm.TryUntilAccepted(&newOp)
	sm.CommitAll(newOp)
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	newOp := Op{Op: "Move", Shard: args.Shard, GID: args.GID}
	sm.TryUntilAccepted(&newOp)
	sm.CommitAll(newOp)
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	newOp := Op{Op: "Query", Num: args.Num}
	sm.TryUntilAccepted(&newOp)
	sm.CommitAll(newOp)
	reply.Config = sm.requests[newOp.ID].(Config)
	return nil
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

func (sm *ShardMaster) TryUntilAccepted(newOp *Op) {
	// Keep trying new sequence slots until successfully committed to log.
	seq := sm.px.Max() + 1
	newOp.ID = nrand()
	deOp := *newOp
	sm.px.Start(seq, deOp)
	to := 5 * time.Millisecond
	for {
		status, untypedOp := sm.px.Status(seq)
		if status {
			op := untypedOp.(Op)
			if op.ID == newOp.ID {
				return
			} else {
				seq += 1
				// seq = sm.px.Max() + 1
				sm.px.Start(seq, deOp)
			}
		}
		time.Sleep(to)
		if to < 100*time.Millisecond {
			to *= 2
		}
	}
}

func (sm *ShardMaster) CommitAll(op Op) {
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
		sm.Commit(newOp)
		sm.latestSeq = i

		if strings.Contains(sm.px.GetPeers()[sm.px.GetMe()], "basic-1") {
			DPrintf("DONE COMITTING %v, min= %v, dones= %+v", i, sm.px.Min(), sm.px.GetDone())
		}
		sm.px.Done(i)
	}
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
	sm.peers = servers
	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}
	sm.previous = map[int64]bool{}
	sm.latestSeq = -1
	sm.requests = make(map[int64]interface{})
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
