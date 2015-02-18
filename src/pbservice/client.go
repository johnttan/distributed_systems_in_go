package pbservice

import "viewservice"
import "net/rpc"
import "fmt"
import "crypto/rand"
import "math/big"

import "time"

type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here
	primary string
	me      string
}

// this may come in handy.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(vshost string, me string) *Clerk {
	fmt.Println("")
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here
	ck.me = me
	ck.primary = ck.vs.Primary()
	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	// fmt.Println(err)
	return false
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {

	// Your code here.

	var args *GetArgs
	var reply *GetReply
	var finished bool

	for !finished {
		if ck.primary != "" {
			args = &GetArgs{key}
			reply = &GetReply{}
			call(ck.primary, "PBServer.Get", args, reply)
			// fmt.Println(ck.primary, reply)
			if reply.Err == OK || reply.Err == ErrNoKey {
				finished = true
			}
		}
		if !finished {
			ck.primary = ck.vs.Primary()
		}
	}
	return reply.Value
}

//
// send a Put or Append RPC
//
func (ck *Clerk) PutAppend(key string, value string, op string) string {

	var args *PutAppendArgs
	var reply *PutAppendReply
	var finished bool
	count := 0
	rand := nrand()
	for !finished {
		// time.Sleep(100 * time.Millisecond)
		if ck.primary != "" {
			args = &PutAppendArgs{Key: key, Value: value, Op: op, Id: rand}
			reply = &PutAppendReply{}
			call(ck.primary, "PBServer.PutAppendReplicate", args, reply)
			if reply.Err == OK {
				finished = true
			}
		}
		if !finished {
			ck.primary = ck.vs.Primary()
		}
		if count > 10000 {
			fmt.Println("RETRYING", ck.primary, key)
			time.Sleep(100 * time.Millisecond)
		}
		count++
	}
	return reply.PreviousValue
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}

//
// tell the primary to append to key's value
// and return the old value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, APPEND)
}
