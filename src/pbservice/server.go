package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "os"
import "syscall"
import "math/rand"

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool // for testing
	unreliable bool // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	viewNum uint
	store   map[string]string
	// 0 for unassigned
	// 1 for backup
	// 2 for primary
	state     uint
	uniqueIds map[int64]*PutAppendReply
	view      viewservice.View
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	if pb.state == 2 {
		if pb.store[args.Key] == "" {
			reply.Err = ErrNoKey
		} else {
			reply.Err = OK
			reply.Value = pb.store[args.Key]
		}
	} else {
		reply.Err = ErrWrongServer
	}
	return nil
}

// This should most likely be broken up.
func (pb *PBServer) PutAppendReplicate(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	// fmt.Println(args)
	// Decide between puts, appends, and replicates.
	// fmt.Println("UNIQUE", args.Value, args.Id, pb.uniqueIds[args.Id])
	reply.Err = OK
	if pb.state == 2 && (pb.uniqueIds[args.Id] == nil || pb.uniqueIds[args.Id].Viewnum != pb.viewNum) {
		reply.PreviousValue = pb.store[args.Key]
		if args.Op == PUT {
			pb.store[args.Key] = args.Value
		} else if args.Op == APPEND {
			pb.store[args.Key] += args.Value
			// fmt.Println("APPEND", args.Key, reply.PreviousValue, "previous", args.Id, "current", args.Value, pb.store[args.Key])
		}
		if pb.view.Backup != "" {
			repArgs := &PutAppendArgs{Key: args.Key, Value: pb.store[args.Key], Op: REPLICATE, Id: args.Id}
			repReply := &PutAppendReply{}
			// BEGIN REPLICATING
			call(pb.view.Backup, "PBServer.PutAppendReplicate", repArgs, repReply)
			if repReply.Err == ErrWrongServer {
				// ERROR REPLICATING
				reply.Err = ErrWrongServer
			}
		}
		reply.Viewnum = pb.viewNum
		pb.uniqueIds[args.Id] = reply

		fmt.Println(args.Id, args.Key, reply.Err, pb.view)
	} else if args.Op == REPLICATE {
		if pb.state == 1 {
			reply.PreviousValue = pb.store[args.Key]
			pb.store[args.Key] = args.Value
		} else {
			reply.Err = ErrWrongServer
		}
	} else if pb.uniqueIds[args.Id] != nil {
		reply.Err = pb.uniqueIds[args.Id].Err
		reply.PreviousValue = pb.uniqueIds[args.Id].PreviousValue
	} else {
		reply.Err = ErrWrongServer
		reply.Viewnum = pb.viewNum
	}
	// if reply.Err == ErrWrongServer {
	// 	fmt.Println(pb.state, pb.me, pb.vs, args)
	// }
	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) Migrate() {
	// pb.mu.Lock()
	if pb.state == 2 {
		args := &MigrationArgs{pb.store}
		reply := &MigrationReply{}
		call(pb.view.Backup, "PBServer.Restore", args, reply)
	}

	// pb.mu.Unlock()
}

func (pb *PBServer) Restore(args *MigrationArgs, reply *MigrationReply) error {
	pb.mu.Lock()
	if pb.state == 0 || pb.state == 1 {
		reply.Err = OK
		pb.store = args.Store
	}
	// fmt.Println("RESTORED", pb.view, pb.me, pb.store)
	fmt.Println("")
	pb.mu.Unlock()
	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	pb.mu.Lock()
	view, err := pb.vs.Ping(pb.viewNum)
	fmt.Println(pb.me, view)
	if err == nil {
		if view.Primary == pb.me {
			pb.state = 2
		} else if view.Backup == pb.me {
			pb.state = 1
		}

		if view.Backup != pb.view.Backup {
			// fmt.Println("MIGRATING", view, pb.view, pb.me)
			pb.viewNum = view.Viewnum
			pb.view = view
			pb.Migrate()
		}

		pb.viewNum = view.Viewnum
		pb.view = view
		// fmt.Println("CURRENT VIEW IN PB", pb.viewNum, pb.view, pb.me, "RECEIVEDVIEW", view)
	}
	pb.mu.Unlock()
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.

	pb.viewNum = 0
	pb.store = make(map[string]string)
	pb.uniqueIds = make(map[int64]*PutAppendReply)
	pb.view = *new(viewservice.View)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l
	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
