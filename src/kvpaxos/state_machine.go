package kvpaxos

// This commit method is core of state machine.
// This is only method that mutates core state.
func (kv *KVPaxos) Commit(op Op, seq int) string {
	defer kv.px.Done(seq)
	defer delete(kv.cache, op.Ack)
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
			kv.cache[op.UID] = kv.data[op.Key]
			kv.data[op.Key] += op.Value
			kv.requests[op.ClientID] = op.ReqID
		}
		previousValue = kv.cache[op.UID]
		return previousValue
	case "Get":
		if reqID := kv.requests[op.ClientID]; op.ReqID > reqID {
			kv.requests[op.ClientID] = op.ReqID
			kv.cache[op.UID] = kv.data[op.Key]
		}
		return kv.cache[op.UID]
	default:
		return ""
	}
}
