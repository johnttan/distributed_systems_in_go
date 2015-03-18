package shardmaster

// This commit method is core of state machine.
// This is the only method that mutates core state.
func (sm *ShardMaster) Commit(op Op) string {
	switch op.Op{
	case: "Join"
		if _, ok := sm.previous[op.GID]; !ok {
			oldConfig := sm.configs[len(sm.configs)-1]
			newGroups := make(map[int64][]string)
			newShards := make([NShards]int64)
// Copy over old maps/arrays
			for gid, servers := range oldConfig.Groups {
				newGroups[gid] = servers
			}
			for shard, gid := range oldConfig.Shards {
				newShards[shard] = gid
			}

			newConfig := Config{Num: len(sm.configs), Shards: newShards, Groups: newGroups}



			newGroups[op.GID] = op.Servers
		}
	case: "Leave"
	case: "Move"
	case: "Query"
	}
	return ""
}
