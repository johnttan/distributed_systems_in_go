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
import "strings"

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
	state       uint
	uniqueIds   map[int64]*PutAppendReply
	view        viewservice.View
	partitioned bool
}

func (pb *PBServer) isPrimary() bool {
	return pb.state == 2
}

func (pb *PBServer) isBackup() bool {
	return pb.state == 1
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	if pb.isPrimary() && !pb.partitioned {
		if pb.store[args.Key] == "" {
			reply.Err = ErrNoKey
		} else {
			reply.Err = OK
			reply.Value = pb.store[args.Key]
		}
	} else {
		reply.Err = ErrWrongServer
	}
	if strings.Index(pb.view.Primary, "part1") > -1 {
		// fmt.Println("GET RECEIVED", args, pb.view, reply)
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
	if pb.uniqueIds[args.Id] != nil && pb.uniqueIds[args.Id].Viewnum != pb.viewNum {
		pb.uniqueIds[args.Id] = nil
	}
	temp := pb.store[args.Key]
	// If primary and not unique
	if pb.isPrimary() && pb.uniqueIds[args.Id] == nil {
		reply.PreviousValue = pb.store[args.Key]
		switch args.Op {
		case PUT:
			temp = args.Value
		case APPEND:
			temp += args.Value
		}
		if pb.view.Backup != "" {
			repArgs := &PutAppendArgs{Key: args.Key, Value: temp, Op: REPLICATE, Id: args.Id}
			repReply := &PutAppendReply{}
			// BEGIN REPLICATING
			call(pb.view.Backup, "PBServer.PutAppendReplicate", repArgs, repReply)
			if repReply.Err == ErrWrongServer {
				reply.Err = ErrWrongServer
			}
		}
		reply.Viewnum = pb.viewNum
		// fmt.Println("SETTING CACHE", reply, pb.me, pb.view)
		if reply.Err == OK {
			pb.uniqueIds[args.Id] = reply
		}

	} else if args.Op == REPLICATE {
		if pb.isBackup() {
			reply.PreviousValue = pb.store[args.Key]
			temp = args.Value
			reply.Viewnum = pb.viewNum
		} else {
			reply.Err = ErrWrongServer
		}
		// fmt.Println("SETTING CACHE", reply, pb.me, pb.view)
		if reply.Err == OK {
			pb.uniqueIds[args.Id] = reply
		}
	} else if pb.uniqueIds[args.Id] != nil {
		reply.Err = pb.uniqueIds[args.Id].Err
		reply.PreviousValue = pb.uniqueIds[args.Id].PreviousValue
		if strings.Index(pb.view.Primary, "rc") > -1 {
			// fmt.Println("SERVING CACHE", pb.uniqueIds[args.Id], "IN", pb.me, pb.view)
		}
	} else {
		reply.Err = ErrWrongServer
		reply.Viewnum = pb.viewNum
	}
	// if reply.Err == ErrWrongServer {
	// 	fmt.Println(pb.state, pb.me, pb.vs, args)
	// }
	// COMMIT CHANGE
	if pb.partitioned {
		reply.Err = ErrWrongServer
	}

	if reply.Err == OK {
		pb.store[args.Key] = temp
	}
	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) Migrate() {
	if pb.isPrimary() {
		args := &MigrationArgs{pb.store, pb.uniqueIds}
		reply := &MigrationReply{}
		call(pb.view.Backup, "PBServer.Restore", args, reply)
	}

}

func (pb *PBServer) Restore(args *MigrationArgs, reply *MigrationReply) error {
	pb.mu.Lock()
	if pb.state == 0 || pb.isBackup() {
		reply.Err = OK
		pb.store = args.Store
		pb.uniqueIds = args.UniqueIds
		for id, reply := range pb.uniqueIds {
			if reply.Viewnum != pb.viewNum {
				pb.uniqueIds[id] = nil
			}
		}
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

	if strings.Index(pb.view.Primary, "part1") > -1 {
		// fmt.Println("BEFORETICK", pb.view, pb.me)
	}
	view, err := pb.vs.Ping(pb.viewNum)
	if strings.Index(pb.view.Primary, "part1") > -1 {
		// fmt.Println("AFTER PING", view, "ERR", err, pb.me)
	}
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
		pb.partitioned = false
	} else {
		fmt.Println("PARTITIONED", pb.me)
		pb.partitioned = true
	}
	if strings.Index(view.Primary, "part1") > -1 {
		// fmt.Println("AFTERTICK", pb.view, pb.me)
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
