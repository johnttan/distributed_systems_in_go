package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
)

type Err string

type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	ReqID    int64
	ClientID int64
	Config   shardmaster.Config
}

type PutAppendReply struct {
	Err           Err
	PreviousValue string // For Append
}

type GetArgs struct {
	Key      string
	ReqID    int64
	ClientID int64
	Config   shardmaster.Config
}

type GetReply struct {
	Err   Err
	Value string
}

const NShards = 10

type Config struct {
	Num    int                // config number
	Shards [NShards]int64     // gid
	Groups map[int64][]string // gid -> servers[]
}

type RequestKVArgs struct {
}
