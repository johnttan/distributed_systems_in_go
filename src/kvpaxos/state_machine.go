package kvpaxos

// This commit method is core of state machine.
// This is the only method that mutates core state.
func (kv *KVPaxos) Commit(op Op) (string, int64) {
	var result string
	switch op.Op {
	case "Put":
		// DPrintf("Applying PUT, currentState: STATE: %+v, OP: %+v, ME: %v, requests: %+v, okreq: %v, idBeforeChange: %v", kv.data[op.Key], op, kv.me, kv.requests)
		kv.data[op.Key] = op.Value
		result = ""
		// DPrintf("After STATE: %+v, me: %v", kv.data[op.Key], kv.me)
	case "Append":
		// DPrintf("Applying APPEND, currentState: STATE: %+v, OP: %+v, ME: %v, requests: %+v, okreq: %v, idBeforeChange: %v", kv.data[op.Key], op, kv.me, kv.requests)
		result = kv.data[op.Key]
		kv.data[op.Key] += op.Value
		// DPrintf("After STATE: %+v, me: %v", kv.data[op.Key], kv.me)
	case "Get":
		// DPrintf("Applying GET, currentState: STATE: %+v, OP: %+v, ME: %v, requests: %+v, okreq: %v, idBeforeChange: %v", kv.data[op.Key], op, kv.me, kv.requests)
		result = kv.data[op.Key]
	}
	return result, op.ClientID
}
