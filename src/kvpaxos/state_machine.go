package kvpaxos

// This commit method is core of state machine.
// This is the only method that mutates core state.
func (kv *KVPaxos) Commit(op Op, seq int) string {
	defer kv.px.Done(seq)
	id, okreq := kv.requests[op.ClientID]
	if (okreq && op.ReqID > id) || !okreq {
		switch op.Op {
		case "Put":
			kv.data[op.Key] = op.Value
			kv.requests[op.ClientID] = op.ReqID
			break
		case "Append":
			kv.cache[op.ClientID] = kv.data[op.Key]
			kv.data[op.Key] += op.Value
			kv.requests[op.ClientID] = op.ReqID
			break
		case "Get":
			kv.requests[op.ClientID] = op.ReqID
			kv.cache[op.ClientID] = kv.data[op.Key]
			break
		}
	}
	value, okcache := kv.cache[op.ClientID]

	if okcache {
		return value
	} else {
		return ""
	}

}
