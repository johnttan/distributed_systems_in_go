package kvpaxos

// This commit method is core of state machine.
// This is the only method that mutates core state.
func (kv *KVPaxos) Commit(op Op, seq int) {
	defer kv.px.Done(seq)
	id, okreq := kv.requests[op.ClientID]
	// If requestID from client is greater, it means it is fresh req, otherwise it is old request and cache should be served.
	if !okreq || op.ReqID > id {
		kv.requests[op.ClientID] = op.ReqID
		switch op.Op {
		case "Put":
			DPrintf("Applying PUT, currentState: OP: %+v, ME: %v", op, kv.me)
			kv.data[op.Key] = op.Value
			kv.cache[op.ClientID] = ""
		case "Append":
			DPrintf("Applying APPEND, currentState: OP: %+v, ME: %v", op, kv.me)
			kv.cache[op.ClientID] = kv.data[op.Key]
			kv.data[op.Key] += op.Value
		case "Get":
			kv.cache[op.ClientID] = kv.data[op.Key]
		}
	}
}
