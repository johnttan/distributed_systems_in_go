package kvpaxos

import "net/rpc"
import "crypto/rand"
import "math/big"
import "time"

type Clerk struct {
	servers    []string
	nextServer int
	id         int64
	reqID      int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.id = nrand()
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

	return false
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	ck.reqID++
	success := false
	var reply *GetReply
	var args *GetArgs
	timeout := 10 * time.Millisecond

	for !success {
		time.Sleep(timeout)
		args = &GetArgs{key, ck.reqID, ck.id}
		reply = &GetReply{}

		success = call(ck.servers[ck.nextServer], "KVPaxos.Get", args, reply)
		ck.nextServer = (ck.nextServer + 1) % len(ck.servers)
		if timeout < 2000*time.Millisecond {
			timeout *= 2
		}
	}

	return reply.Value
}

//
// shared by Put and Append.
//
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	ck.reqID++
	// fmt.Println("start", key, op)
	success := false
	var reply *PutAppendReply
	var args *PutAppendArgs
	timeout := 10 * time.Millisecond

	for !success {
		time.Sleep(timeout)
		args = &PutAppendArgs{key, value, op, ck.reqID, ck.id}
		reply = &PutAppendReply{}

		success = call(ck.servers[ck.nextServer], "KVPaxos.PutAppend", args, reply)
		ck.nextServer = (ck.nextServer + 1) % len(ck.servers)
		if timeout < 2000*time.Millisecond {
			timeout *= 2
		}
	}

	return reply.PreviousValue
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) string {
	v := ck.PutAppend(key, value, "Append")
	return v
}
