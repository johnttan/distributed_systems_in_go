package shardkv

import "shardmaster"

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

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key            string
	Value          string
	Op             string
	ReqID          int64
	ClientID       int64
	UID            int64
	MigrationReply *RequestKVReply
	Config         shardmaster.Config
	Shard          int
}

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

type RequestKVArgs struct {
	Shard  int
	Config shardmaster.Config
}

type RequestKVReply struct {
	Cache    map[int64]string
	Requests map[int64]int64
	Data     map[string]string
	Err      Err
}
