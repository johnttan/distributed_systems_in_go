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
import "errors"

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
	migrate     bool
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

func (pb *PBServer) hasBackup() bool {
	return pb.view.Backup != ""
}

func (pb *PBServer) isCached(args *PutAppendArgs) bool {
	return pb.uniqueIds[args.Id] != nil
}

func (pb *PBServer) CheckGet(args *GetArgs, reply *GetReply) error {
	if pb.isPrimary() {
		reply.Err = ErrWrongServer
		return errors.New("I'm primary")
	}
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	reply.Err = ErrWrongServer
	if pb.partitioned || !pb.isPrimary() {
		pb.mu.Unlock()
		return errors.New("Partitioned")
	}
	res := true
	if pb.hasBackup() {
		checkArgs := &GetArgs{}
		checkReply := &GetReply{}
		res = call(pb.view.Backup, "PBServer.CheckGet", checkArgs, checkReply)
	}
	if res {
		reply.Value = pb.store[args.Key]
		if reply.Value == "" {
			reply.Err = ErrNoKey
		} else {
			reply.Err = OK
		}
		pb.mu.Unlock()
		return nil
	}
	pb.mu.Unlock()
	return errors.New("NOT PRIMARY")
}

func (pb *PBServer) Replicate(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	reply.Err = OK
	if pb.isBackup() || pb.isUnassigned() {
		for key, val := range args.Store {
			pb.store[key] = val
		}
		if args.UniqueIds != nil {
			for key, val := range args.UniqueIds {
				pb.uniqueIds[key] = val
			}
		}
		pb.mu.Unlock()
		return nil
	} else {
		reply.Err = ErrWrongServer
		pb.mu.Unlock()
		return errors.New("NOT REPLICATING")
	}
}

// This should most likely be broken up.
func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	if pb.partitioned || !pb.isPrimary() {
		pb.mu.Unlock()
		return errors.New("Partitioned or not primary")
	}
	if pb.isCached(args) {
		reply.Err = pb.uniqueIds[args.Id].Err
		reply.PreviousValue = pb.uniqueIds[args.Id].PreviousValue
		pb.mu.Unlock()
		return nil
	}
	// Save temp before committing
	temp := pb.store[args.Key]
	reply.PreviousValue = pb.store[args.Key]
	success := true
	// Decide between puts, appends, and replicates.
	switch {
	case args.Op == PUT:
		temp = args.Value
	case args.Op == APPEND:
		temp += args.Value
	}
	if pb.hasBackup() {
		args.Store = make(map[string]string)
		args.Store[args.Key] = temp
		success = call(pb.view.Backup, "PBServer.Replicate", args, reply)
	}

	switch {
	case success: //Only commit to store if successful replication
		// fmt.Println("COMMITTING", temp, args, pb.me, pb.view)
		reply.Err = OK
		pb.store[args.Key] = temp
		pb.uniqueIds[args.Id] = reply
		pb.mu.Unlock()
		return nil
	default:
		pb.mu.Unlock()
		reply.Err = ErrWrongServer
		return errors.New("FAILED")
	}
}

func (pb *PBServer) Migrate(backup string) bool {
	args := &PutAppendArgs{Store: pb.store, UniqueIds: pb.uniqueIds}
	reply := &PutAppendReply{}
	return call(backup, "PBServer.Replicate", args, reply)
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
	case (view.Backup != pb.view.Backup || pb.migrate) && pb.isPrimary():
		res := pb.Migrate(view.Backup)
		// If migration failed due to unreliable connection, retry migrate on next tick.
		if !res {
			fmt.Println("MIGRATION FAILED")
			pb.migrate = true
		} else {
			pb.migrate = false
		}
	case (pb.isPrimary() && view.Primary != pb.me) || (pb.isBackup() && view.Backup != pb.me):
		// Changed from primary to backup. Have to invalidate cache, otherwise will respond to client requests
		fmt.Println("INVALIDATING CACHE")
		pb.uniqueIds = make(map[int64]*PutAppendReply)
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
