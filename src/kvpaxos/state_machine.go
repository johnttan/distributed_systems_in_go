package kvpaxos

func (kv *KVPaxos) Commit(op Op) string {
	switch op.Op {
	case "Put":
		kv.data[op.Key] = op.Value
		return ""
	case "Append":
		previousValue := kv.data[op.Key]
		kv.data[op.Key] += op.Value
		return previousValue
	case "Get":
		return kv.data[op.Key]
	default:
		return ""
	}
}
