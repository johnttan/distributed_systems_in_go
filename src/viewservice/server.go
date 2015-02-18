package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type Node struct {
	id string
	// Node.state
	// 0 = dead
	// 1 = available but not backup/primary
	// 2 = backup
	// 3 = primary
	state          uint
	ticksSincePing uint
	viewNum        uint
	ack            bool
}

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     bool  // for testing
	rpccount int32 // for testing
	me       string

	view *View

	nodes map[string]*Node
	// Your declarations here.
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()
	var node *Node
	node = vs.nodes[args.Me]
	if node == nil {
		node = new(Node)
		vs.nodes[args.Me] = node
		node.state = 1
		node.viewNum = args.Viewnum
		node.id = args.Me

		if !vs.hasPrimary() || !vs.hasBackup() {
			vs.newView()
		}
	} else if node.viewNum > args.Viewnum {
		node.state = 1
		fmt.Println("DETECTED RESTARTED SERVER", args.Me, args.Viewnum)
	} else {
		node.viewNum = args.Viewnum
	}

	node.ticksSincePing = 0

	reply.View = *vs.view

	if node.viewNum == vs.view.Viewnum {
		node.ack = true
	}
	vs.mu.Unlock()
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	reply.View = *vs.view
	return nil
}

func (vs *ViewServer) hasPrimary() bool {
	return vs.view.Primary != ""
}

func (vs *ViewServer) hasBackup() bool {
	return vs.view.Backup != ""
}

func (vs *ViewServer) eligibleForNewView() bool {
	return !(vs.hasPrimary() && !vs.nodes[vs.view.Primary].ack)
}

func (vs *ViewServer) newView() {
	// Don't create new view and mutate state if primary hasn't acked.
	if vs.eligibleForNewView() {
		if vs.hasPrimary() && vs.nodes[vs.view.Primary].state <= 1 && vs.hasBackup() {
			vs.view.Primary = vs.view.Backup
			vs.nodes[vs.view.Backup].state = 3
			vs.view.Backup = ""
		}
		if vs.hasBackup() && vs.nodes[vs.view.Backup].state <= 1 {
			vs.view.Backup = ""
		}

		vs.view.Viewnum += 1
		for _, node := range vs.nodes {
			node.ack = false
			if node.state == 1 && !vs.hasPrimary() {
				vs.view.Primary = node.id
				node.state = 3
			}
			if node.state == 1 && !vs.hasBackup() {
				vs.view.Backup = node.id
				node.state = 2
			}
		}
	}
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	for _, node := range vs.nodes {
		node.ticksSincePing += 1
		if node.ticksSincePing >= DeadPings {
			node.state = 0
			if vs.view.Backup == node.id {
				vs.view.Backup = ""
				vs.newView()
			}
			if vs.view.Primary == node.id && vs.hasBackup() && node.ack && vs.nodes[vs.view.Backup].ack {
				// Checks that dead primary is synced. Cannot advanced to next view if not synced.
				// Checks that backup node is initialized and synced
				// fmt.Println("PROMOTED", vs.view.Primary)
				vs.newView()
			}
		}

		if node.state == 1 && vs.view.Primary == node.id && node.ack {
			vs.newView()
		}
		if node.state == 1 && !vs.hasBackup() {
			vs.newView()
		}
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	// fmt.Println("KILLING VS", vs.me)
	vs.dead = true
	vs.l.Close()
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.nodes = make(map[string]*Node)
	vs.view = new(View)
	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
