package kvpaxos

// This commit method is core of state machine.
// This is only method that mutates core state.
func (kv *KVPaxos) Commit(op Op, seq int) string {
	kv.px.Done(seq)
	switch op.Op {
	case "Put":
		kv.data[op.Key] = op.Value
		kv.requests[op.UID] = ""
		delete(kv.requests, op.Ack)
		return ""
	case "Append":
		previousValue := kv.data[op.Key]
		kv.data[op.Key] += op.Value
		kv.requests[op.UID] = previousValue
		delete(kv.requests, op.Ack)
		return previousValue
	case "Get":
		kv.requests[op.UID] = kv.data[op.Key]
		delete(kv.requests, op.Ack)
		return kv.data[op.Key]
	default:
		return ""
	}
}
