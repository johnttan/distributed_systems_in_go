package kvpaxos

// This commit method is core of state machine.
// This is the only method that mutates core state.
func (kv *KVPaxos) Commit(op Op, seq int) string {
	defer kv.px.Done(seq)
	switch op.Op {
	case "Put":
		if reqID := kv.requests[op.ClientID]; op.ReqID > reqID {
			kv.data[op.Key] = op.Value
			kv.requests[op.ClientID] = op.ReqID
		}
		return ""
	case "Append":
		var previousValue string
		if reqID := kv.requests[op.ClientID]; op.ReqID > reqID {
			kv.cache[op.ClientID] = kv.data[op.Key]
			kv.data[op.Key] += op.Value
			kv.requests[op.ClientID] = op.ReqID
		}
		previousValue = kv.cache[op.ClientID]
		return previousValue
	case "Get":
		if reqID := kv.requests[op.ClientID]; op.ReqID > reqID {
			kv.requests[op.ClientID] = op.ReqID
			kv.cache[op.ClientID] = kv.data[op.Key]
		}
		return kv.cache[op.ClientID]
	default:
		return ""
	}
}
