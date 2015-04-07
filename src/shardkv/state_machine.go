package shardkv

// This commit method is core of state machine.
// This is the only method that mutates core state.
func (kv *ShardKV) Commit(op Op) string {
	switch op.Op {
	case "Put":
		DPrintf(kv.me, "Applying PUT, currentState: STATE: %+v, OP: %+v", kv.data, op)
		kv.data[op.Key] = op.Value
		return ""
	case "Append":
		DPrintf(kv.me, "Applying APPEND, currentState: STATE: %+v, OP: %+v, ME: %v", kv.data, op, kv.me)
		before := kv.data[op.Key]
		kv.data[op.Key] += op.Value
		return before
	case "Get":
		return kv.data[op.Key]
	case "Config":
		DPrintf(kv.me, "CONFIG IN STATE MACHINE  %+v", op.Config)
		kv.config = op.Config
	}
	return ""
}
