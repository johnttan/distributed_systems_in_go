package kvpaxos

// This commit method is core of state machine.
// This is only method that mutates core state.
func (kv *KVPaxos) Commit(op Op, seq int) string {
	kv.px.Done(seq)
	switch op.Op {
	case "Put":
		kv.data[op.Key] = op.Value
		kv.requests[op.UID] = ""
		return ""
	case "Append":
		previousValue := kv.data[op.Key]
		kv.data[op.Key] += op.Value
		kv.requests[op.UID] = previousValue
		return previousValue
	case "Get":
		kv.requests[op.UID] = kv.data[op.Key]
		return kv.data[op.Key]
	case "Ack":
		// Clean up requests cache.
		delete(kv.requests, op.UID)
		return ""
	default:
		return ""
	}
}
