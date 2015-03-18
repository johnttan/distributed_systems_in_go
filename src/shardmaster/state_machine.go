package shardmaster

// This commit method is core of state machine.
// This is the only method that mutates core state.
func (sm *ShardMaster) Commit(op Op) string {
	switch op.Op {
	case "Join":
		if _, ok := sm.previous[op.GID]; !ok {
			oldConfig := sm.configs[len(sm.configs)-1]
			newGroups := make(map[int64][]string)
			newGroups[op.GID] = op.Servers
			newShards := [NShards]int64{}
			// Copy over old maps/arrays
			for gid, servers := range oldConfig.Groups {
				newGroups[gid] = servers
			}
			for shard, gid := range oldConfig.Shards {
				newShards[shard] = gid
			}
			newConfig := Config{Num: len(sm.configs), Shards: newShards, Groups: newGroups}

			// Find # of shards per group needed to be balanced
			shardsPerGroup := NShards / len(newGroups)
			// Keep track of how many reassigned to new group
			assignedToNew := 0
			// Find shards not assigned to new group, and if new group still needs shards, assign current shard to new group
			for shard, gid := range newShards {
				if gid != op.GID && assignedToNew < shardsPerGroup {
					newShards[shard] = op.GID
				}
			}
			sm.configs = append(sm.configs, newConfig)
		}
	case "Leave":
	case "Move":
	case "Query":
	}
	return ""
}
