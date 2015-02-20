package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     bool  // for testing
	rpccount int32 // for testing
	me       string

	view         *View
	primaryAck   uint
	backupAck    uint
	primaryTicks uint
	backupTicks  uint
}

func (vs *ViewServer) Promote() {
	vs.view.Primary = vs.view.Backup
	vs.primaryAck = vs.backupAck
	vs.backupAck = 0
	vs.view.Viewnum++
	vs.view.Backup = ""
	vs.primaryTicks = vs.backupTicks
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()
	if args.Viewnum < vs.primaryAck && vs.view.Primary == args.Me {
		vs.Promote()
	}
	if vs.view.Primary == args.Me {
		vs.primaryAck = args.Viewnum
		vs.primaryTicks = 0
	} else if vs.view.Backup == args.Me {
		vs.backupAck = args.Viewnum
		vs.backupTicks = 0
	}

	if !vs.hasPrimary() && vs.eligibleForNewView() {
		vs.view.Primary = args.Me
		vs.primaryAck = args.Viewnum
		vs.view.Viewnum++
	} else if !vs.hasBackup() && vs.eligibleForNewView() && vs.view.Primary != args.Me {
		vs.view.Backup = args.Me
		vs.backupAck = args.Viewnum
		vs.view.Viewnum++
	}
	reply.View = *vs.view
	vs.mu.Unlock()
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
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
	return vs.primaryAck == vs.view.Viewnum
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	vs.mu.Lock()
	if vs.hasBackup() {
		vs.backupTicks += 1
	}
	if vs.hasPrimary() {
		vs.primaryTicks += 1
	}
	if vs.backupTicks >= DeadPings && vs.eligibleForNewView() {
		vs.view.Backup = ""
		vs.backupTicks = 0
		vs.view.Viewnum++
	}
	if (vs.primaryTicks >= DeadPings || !vs.hasPrimary()) && vs.hasBackup() && vs.eligibleForNewView() {
		vs.Promote()
		vs.backupTicks = 0
	}
	vs.mu.Unlock()
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
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
