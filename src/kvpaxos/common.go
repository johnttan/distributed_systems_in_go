package kvpaxos

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	UID   int64
	Ack   int64
}

type PutAppendReply struct {
	Err           Err
	PreviousValue string // For Append
}

type GetArgs struct {
	Key string
	UID int64
	Ack int64
}

type GetReply struct {
	Err   Err
	Value string
}

// type AckArgs struct {
// 	UID int64
// }

// type AckReply struct {
// 	Err Err
// }
