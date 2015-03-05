package kvpaxos

// This commit method is core of state machine.
// This is only method that mutates core state.
func (kv *KVPaxos) Commit(op Op, seq int) string {
	defer kv.px.Done(seq)
	// defer delete(kv.requests, op.Ack)
	switch op.Op {
	case "Put":
		if _, ok := kv.requests[op.UID]; !ok {
			kv.data[op.Key] = op.Value
			kv.requests[op.UID] = ""
		}
		return ""
	case "Append":
		var previousValue string
		if _, ok := kv.requests[op.UID]; !ok {
			previousValue = kv.data[op.Key]
			kv.data[op.Key] += op.Value
			kv.requests[op.UID] = previousValue
		} else {
			previousValue = kv.requests[op.UID]
		}
		return previousValue
	case "Get":
		if _, ok := kv.requests[op.UID]; !ok {
			kv.requests[op.UID] = kv.data[op.Key]
		}
		value := kv.requests[op.UID]
		return value
	default:
		return ""
	}
}
