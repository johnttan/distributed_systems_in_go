package pbservice

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongServer  = "ErrWrongServer"
	PUT             = "PUT"
	APPEND          = "APPEND"
	REPLICATEPUT    = "REPLICATEPUT"
	REPLICATEAPPEND = "REPLICATEAPPEND"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string
	Id        int64
	Store     map[string]string
	UniqueIds map[int64]*PutAppendReply
}

type PutAppendReply struct {
	Err           Err
	PreviousValue string
	Viewnum       uint
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}
