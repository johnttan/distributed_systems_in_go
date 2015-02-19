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
	state       uint
	uniqueIds   map[int64]*PutAppendReply
	view        viewservice.View
	partitioned bool
}

func (pb *PBServer) isPrimary() bool {
	return pb.view.Primary == pb.me
}

func (pb *PBServer) isBackup() bool {
	return pb.view.Backup == pb.me
}

func (pb *PBServer) isUnassigned() bool {
	return !pb.isPrimary() && !pb.isBackup()
}

func (pb *PBServer) isCached(args *PutAppendArgs) bool {
	return pb.uniqueIds[args.Id] != nil
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
	return nil
}

func (pb *PBServer) SendToBackup(key string, value string, id int64) Err {
	repReply := &PutAppendReply{}
	repReply.Err = OK
	if pb.view.Backup != "" {
		repArgs := &PutAppendArgs{Key: key, Value: value, Op: REPLICATE, Id: id}
		// BEGIN REPLICATING
		call(pb.view.Backup, "PBServer.PutAppendReplicate", repArgs, repReply)
	}
	return repReply.Err
}

// This should most likely be broken up.
func (pb *PBServer) PutAppendReplicate(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	reply.Err = OK
	// Invalidate cache if viewnum has changed.
	if pb.uniqueIds[args.Id] != nil && pb.uniqueIds[args.Id].Viewnum != pb.viewNum {
		pb.uniqueIds[args.Id] = nil
	}
	// Save temp before committing
	temp := pb.store[args.Key]

	reply.PreviousValue = pb.store[args.Key]
	// Decide between puts, appends, and replicates.
	switch {
	case !pb.isCached(args):
		switch {
		case args.Op == PUT && pb.isPrimary():
			temp = args.Value
			reply.Err = pb.SendToBackup(args.Key, temp, args.Id)
			reply.Viewnum = pb.viewNum
			break
		case args.Op == APPEND && pb.isPrimary():
			temp += args.Value
			reply.Err = pb.SendToBackup(args.Key, temp, args.Id)
			reply.Viewnum = pb.viewNum
			if reply.Err == OK {
				pb.uniqueIds[args.Id] = reply
			}
			break
		case args.Op == REPLICATE && pb.isBackup():
			temp = args.Value
			reply.Viewnum = pb.viewNum
			pb.uniqueIds[args.Id] = reply
			break
		default:
			reply.Err = ErrWrongServer
		}
	default:
		reply.Err = pb.uniqueIds[args.Id].Err
		reply.PreviousValue = pb.uniqueIds[args.Id].PreviousValue
	}

	switch {
	case pb.partitioned:
		reply.Err = ErrWrongServer
	case reply.Err == OK: //Only commit to store if OK
		pb.store[args.Key] = temp
	}

	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) Migrate(backup string) {
	if pb.isPrimary() {
		args := &MigrationArgs{pb.store, pb.uniqueIds}
		reply := &MigrationReply{}
		call(backup, "PBServer.Restore", args, reply)
	}

}

func (pb *PBServer) Restore(args *MigrationArgs, reply *MigrationReply) error {
	pb.mu.Lock()
	if pb.isUnassigned() || pb.isBackup() {
		reply.Err = OK
		pb.store = args.Store
		pb.uniqueIds = args.UniqueIds
		for id, reply := range pb.uniqueIds {
			if reply.Viewnum != pb.viewNum {
				pb.uniqueIds[id] = nil
			}
		}
	}
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

	switch {
	case err != nil:
		pb.partitioned = true
		pb.mu.Unlock()
		return
	case view.Backup != pb.view.Backup:
		pb.Migrate(view.Backup)
	}
	pb.viewNum = view.Viewnum
	pb.view = view
	pb.partitioned = false
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
