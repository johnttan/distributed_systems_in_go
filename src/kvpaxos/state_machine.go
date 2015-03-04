package kvpaxos

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
	default:
		return ""
	}
}
