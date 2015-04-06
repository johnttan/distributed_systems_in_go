package shardkv

// This commit method is core of state machine.
// This is the only method that mutates core state.
func (kv *ShardKV) Commit(op Op) string {
	switch op.Op {
	case "Put":
		// DPrintf("Applying PUT, currentState: STATE: %+v, OP: %+v, ME: %v", kv.data[op.Key], op, kv.me)
		kv.data[op.Key] = op.Value
		return ""
	case "Append":
		// DPrintf("Applying APPEND, currentState: STATE: %+v, OP: %+v, ME: %v", kv.data[op.Key], op, kv.me)
		before := kv.data[op.Key]
		kv.data[op.Key] += op.Value
		return before
	case "Get":
		return kv.data[op.Key]
	case "Config":
		DPrintf("CONFIG IN STATE MACHINE  %+v", op)
	}
	return ""
}
