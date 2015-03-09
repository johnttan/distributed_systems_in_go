package kvpaxos

// This commit method is core of state machine.
// This is the only method that mutates core state.
func (kv *KVPaxos) Commit(op Op) Op {
	switch op.Op {
	case "Put":
		// DPrintf("Applying PUT, currentState: STATE: %+v, OP: %+v, ME: %v", kv.data[op.Key], op, kv.me)
		kv.data[op.Key] = op.Value
		op.PutAppendReply = &PutAppendReply{}
	case "Append":
		// DPrintf("Applying APPEND, currentState: STATE: %+v, OP: %+v, ME: %v", kv.data[op.Key], op, kv.me)
		op.PutAppendReply = &PutAppendReply{PreviousValue: kv.data[op.Key]}
		kv.data[op.Key] += op.Value
	case "Get":
		op.GetReply = &GetReply{Value: kv.data[op.Key]}
	}
	return op
}
