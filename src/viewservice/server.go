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
	state uint
	ticksSincePing uint
	viewNum uint
}

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     bool  // for testing
	rpccount int32 // for testing
	me       string

	currentView *View

	nodes map[string]*Node
	// Your declarations here.
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()
	// New node logic.
	if vs.nodes[args.Me] == nil {
		vs.nodes[args.Me] = new(Node)
		vs.nodes[args.Me].state = 1
		vs.nodes[args.Me].viewNum = args.Viewnum
		vs.nodes[args.Me].id = args.Me

		if vs.currentView.Primary == "" || vs.currentView.Backup == "" {
			vs.newView()
		}
// Detected server that is behind/restarted
	}else if vs.nodes[args.Me].viewNum > args.Viewnum {
		vs.nodes[args.Me].state = 1
		// fmt.Println("DETECTED RESTARTED SERVER", args.Me, args.Viewnum)
	}else{
		// Reset ticks and viewNum
		vs.nodes[args.Me].ticksSincePing = 0
		vs.nodes[args.Me].viewNum = args.Viewnum
	}

	vs.nodes[args.Me].ticksSincePing = 0

	reply.View = *vs.currentView
	// fmt.Println("PING VIEWNUM", args.Me, args.Viewnum, vs.nodes[args.Me].viewNum)
	vs.mu.Unlock()
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.mu.Lock()
	// Your code here.
	reply.View = *vs.currentView
	// fmt.Println("CURRENT BACKUP", vs.currentView.Backup)
	// fmt.Println("CURRENT PRIMARY", vs.currentView.Primary)
	vs.mu.Unlock()
	return nil
}

func (vs *ViewServer) newView() {
	newView := new(View)
	currentPrimary := vs.currentView.Primary
	currentBackup := vs.currentView.Backup
	// fmt.Println("CREATING NEW VIEW", vs.currentView.Backup, vs.currentView.Primary)
	if currentPrimary != "" {
		// fmt.Println("CURRENT PRIMARY IN NEWVIEW", vs.currentView.Primary, vs.nodes[vs.currentView.Primary].state, "BACKUP", vs.currentView.Backup)
		if vs.nodes[vs.currentView.Primary].state > 1 {
			newView.Primary = vs.currentView.Primary
		}else if currentBackup != "" {
			newView.Primary = vs.currentView.Backup
			vs.nodes[vs.currentView.Backup].state = 3
			currentBackup = ""
		}
	}
	if currentBackup != "" {
		if vs.nodes[vs.currentView.Backup].state > 1 {
			newView.Backup = vs.currentView.Backup
		}else {
			newView.Backup = ""
		}
	}

	newView.Viewnum = vs.currentView.Viewnum + 1
	for _, node := range vs.nodes {
		if node.state == 1 && newView.Primary == "" {
			newView.Primary = node.id
			node.state = 3
		}
		if node.state == 1 && newView.Backup == "" {
			newView.Backup = node.id
			node.state = 2
		}
		// fmt.Println("VIEWNUMS IN NEWVIEW", node.id, node.viewNum, vs.currentView.Viewnum)
	}

	if (vs.currentView.Primary != "" && vs.nodes[vs.currentView.Primary].viewNum < vs.currentView.Viewnum) {
		// fmt.Println("WAITING ON VIEW")
	}else{
		vs.currentView = newView
	}

	// fmt.Println("NEW PRIMARY", vs.currentView.Primary)
	// fmt.Println("NEW BACKUP", vs.currentView.Backup)
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	vs.mu.Lock()
	for _, node := range vs.nodes {
		node.ticksSincePing += 1
		if node.ticksSincePing >= DeadPings {
			// fmt.Println("SETTING DEAD", node.id)
			node.state = 0
		}
		if node.state == 1 && vs.currentView.Primary == node.id {
			if node.viewNum == vs.currentView.Viewnum {
				vs.newView()
			}
		}
		if node.state == 0 && vs.currentView.Backup == node.id {
			vs.nodes[vs.currentView.Backup].state = 0
			vs.currentView.Backup = ""
			// fmt.Println("DETECTED BACKUP FAILURE")
			vs.newView()
		}
		if node.state == 0 && vs.currentView.Primary == node.id{
			// Checks that dead primary is synced. Cannot advanced to next view if not synced.
			// Checks that backup node is initialized and synced
			if vs.currentView.Viewnum == node.viewNum && vs.currentView.Viewnum == vs.nodes[vs.currentView.Backup].viewNum{
				vs.currentView.Primary = vs.currentView.Backup
				vs.nodes[vs.currentView.Primary].state = 3
				vs.currentView.Backup = ""
				fmt.Println("PROMOTED", vs.currentView.Primary)
				// fmt.Println("VIEWNUM AFTER PROMOTION", vs.currentView.Primary, vs.currentView.Viewnum, vs.nodes[vs.currentView.Primary].viewNum)
				vs.newView()
			}

		}

		if node.state == 1 && vs.currentView.Backup == "" {
			vs.newView()
		}
		// fmt.Println("LOOPING NODES", node.id, node.state)
	}
	// fmt.Println("IN TICK", vs.currentView.Backup, vs.currentView.Primary)
	vs.mu.Unlock()
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
	vs.currentView = new(View)
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
