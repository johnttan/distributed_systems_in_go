package shardkv

import "shardmaster"
import "net/rpc"
import "time"
import "sync"
import "fmt"
import "crypto/rand"
import "math/big"

type Clerk struct {
	mu     sync.Mutex // one RPC at a time
	sm     *shardmaster.Clerk
	config shardmaster.Config
	// You'll have to modify Clerk.
	id    int64
	reqID int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(shardmasters []string) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(shardmasters)
	// You'll have to modify MakeClerk.
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

	fmt.Println(err)
	return false
}

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	// You'll have to modify Get().
	for {
		ck.reqID++
		// DPrintf(ck.id, "trying client get call again %v", ck.reqID)
		shard := key2shard(key)

		gid := ck.config.Shards[shard]

		servers, ok := ck.config.Groups[gid]

		if ok {
			// try each server in the shard's replication group.
			for _, srv := range servers {
				args := &GetArgs{}
				args.Key = key
				args.ClientID = ck.id
				args.ReqID = ck.reqID
				args.Config = ck.config
				var reply GetReply
				ok := call(srv, "ShardKV.Get", args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					ck.reqID++
					break
				}
			}
		}

		time.Sleep(100 * time.Millisecond)

		// ask master for a new configuration.
		ck.config = ck.sm.Query(-1)
	}
	return ""
}

// send a Put or Append request.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	// You'll have to modify Put().
	for {
		ck.reqID++

		shard := key2shard(key)

		gid := ck.config.Shards[shard]
		// DPrintf(ck.id, "trying client putappend call again k=%v, v=%v, g=%v", key, value, gid)

		servers, ok := ck.config.Groups[gid]

		if ok {
			// try each server in the shard's replication group.
			for _, srv := range servers {
				args := &PutAppendArgs{}
				args.Config = ck.config
				args.Key = key
				args.Value = value
				args.Op = op
				args.ClientID = ck.id
				args.ReqID = ck.reqID
				var reply PutAppendReply
				ok := call(srv, "ShardKV.PutAppend", args, &reply)
				if ok && reply.Err == OK {
					return reply.PreviousValue
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
			}
		}

		time.Sleep(100 * time.Millisecond)

		// ask master for a new configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) string {
	v := ck.PutAppend(key, value, "Append")
	return v
}

func (ck *Clerk) SendShard(servers []string, config shardmaster.Config, shard int, data map[string]string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	for {
		ck.reqID++

		for _, srv := range servers {
			args := &SendShardArgs{
				Shard:  shard,
				Data:   data,
				Config: config,
			}
			reply := &SendShardReply{}

			ok := call(srv, "ShardKV.ReceiveShard", args, reply)
			if ok && reply.Err == OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
