package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string
	Id    int64
	// You'll have to add definitions here.

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err           Err
	PreviousValue string
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}

type ReplicateArgs struct {
	Key string
	Value string
	Op string
	Id int64
}

type ReplicateReply struct {
	Err Err
	PreviousValue string
}
// Your RPC definitions here.
