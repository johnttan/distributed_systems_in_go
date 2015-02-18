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
