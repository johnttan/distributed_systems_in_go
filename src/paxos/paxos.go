package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "math"
import "fmt"
import "math/rand"

const (
	PREPARE_OK     = "PrepareOK"
	PREPARE_REJECT = "PrepareReject"
	ACCEPT_OK      = "AcceptOK"
	ACCEPT_REJECT  = "AcceptReject"
)

type Response string

const debug = true

func PR(format string, args ...interface{}) {
	if debug {
		fmt.Printf(format, args...)
	}
}

type PrepareArgs struct {
	Seq          int
	PrepareValue interface{}
	PrepareNum   int
}
type PrepareReply struct {
	Response           Response
	HighestAcceptNum   int
	HighestAcceptValue interface{}
	Done               int
}

type AcceptArgs struct {
	AcceptNum   int
	AcceptValue interface{}
	Seq         int
}

type AcceptReply struct {
	Response Response
	Done     int
}

type DecideReply struct {
	Response Response
	Done     int
}

type DecideArgs struct {
	Done        int
	DoneMe      int
	DecideValue interface{}
	DecideNum   int
	Seq         int
}

type Instance struct {
	Seq                 int
	HighestPrepareValue interface{}
	HighestPrepareNum   int
	HighestAcceptValue  interface{}
	HighestAcceptNum    int
	DecidedValue        interface{}
	Decided             bool
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int // index into peers[]

	log map[int]*Instance
	// done is indexed by px.me index
	done []int
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	return false
}
func (px *Paxos) newInstance(seq int, where string) {
	if _, ok := px.log[seq]; !ok {
		// fmt.Println("NEW INSTANCE", seq, px.log[seq], where)
		px.log[seq] = &Instance{Seq: seq, HighestPrepareNum: -1, HighestAcceptNum: -1, Decided: false}
	}
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	go px.Propose(seq, v)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	px.mu.Lock()
	defer px.mu.Unlock()
	// fmt.Println("CALLED DONE", seq, "ME", px.me)
	// Your code here.
	if seq > px.done[px.me] {
		px.done[px.me] = seq
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	max := -1
	for seq, _ := range px.log {
		if seq > max {
			max = seq
		}
	}
	return max
}

func (px *Paxos) GetLog() map[int]*Instance {
	return px.log
}

func (px *Paxos) GetPeers() []string {
	return px.peers
}

func (px *Paxos) GetMe() int {
	return px.me
}

func (px *Paxos) GetDone() []int {
	return px.done
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	min := math.MaxUint32
	// fmt.Println("CALLED MIN", px.done, px.me, len(px.done))
	//If all nodes have responded with initial dones.
	for _, seq := range px.done {
		if seq < min {
			min = seq
		}
	}
	for id, _ := range px.log {
		if id < min {
			delete(px.log, id)
		}
	}
	return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
	px.mu.Lock()
	defer px.mu.Unlock()
	if seq < px.Min() {
		return false, nil
	}
	if _, ok := px.log[seq]; ok && px.log[seq].Decided {
		return true, px.log[seq].DecidedValue
	} else {
		return false, nil
	}
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.log = make(map[int]*Instance)
	px.done = make([]int, len(px.peers))
	for i, _ := range px.done {
		px.done[i] = -1
	}
	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
