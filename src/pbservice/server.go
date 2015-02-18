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
	viewNum    uint
	store      map[string]string
// 0 for unassigned
// 1 for backup
// 2 for primary
	state      uint
	uniqueIds map[int64]bool
}


func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	if pb.state == 2 {
		if pb.store[args.Key] == "" {
			reply.Err = ErrNoKey
		}else {
			reply.Err = OK
			reply.Value = pb.store[args.Key]
		}
	} else {
		reply.Err = ErrWrongServer
	}

	return nil
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// fmt.Println(args)
	// Your code here.
	if pb.state == 2 {
		reply.PreviousValue = pb.store[args.Key]
		reply.Err = OK
		if args.Op == "Put" && !pb.uniqueIds[args.Id] {
			pb.store[args.Key] = args.Value
		} else if args.Op == "Append" {
			pb.store[args.Key] += args.Value
		}
		pb.uniqueIds[args.Id] = true
	} else {
		reply.Err = ErrWrongServer
	}

	return nil
}


//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	view, err := pb.vs.Ping(pb.viewNum)
	if err == nil {
		if view.Primary == pb.me {
			pb.state = 2
		} else if view.Backup == pb.me {
			pb.state = 1
		}
		pb.viewNum = view.Viewnum
	}
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

	pb.viewNum = 0;
	pb.store = make(map[string]string)
	pb.uniqueIds = make(map[int64]bool)
	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l
	fmt.Println(pb)

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
