package shardmaster

// This commit method is core of state machine.
// This is the only method that mutates core state.
func (sm *ShardMaster) Commit(op Op) string {
	switch op.Op {
	case "Put":
		sm.data[op.Key] = op.Value
		return ""
	case "Append":
		before := sm.data[op.Key]
		sm.data[op.Key] += op.Value
		return before
	case "Get":
		return sm.data[op.Key]
	}
	return ""
}
