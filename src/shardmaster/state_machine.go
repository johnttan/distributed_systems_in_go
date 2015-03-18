package shardmaster

// This commit method is core of state machine.
// This is the only method that mutates core state.
func (sm *ShardMaster) Commit(op Op) {
	DPrintf("COMMITTING %+v", op)

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

			// Find # of shards per group needed to be balanced
			shardsPerGroup := NShards / len(newGroups)
			// Keep track of how many reassigned to new group
			assignedToNew := 0
			// Find shards not assigned to new group, and if new group still needs shards, assign current shard to new group
			for shard, gid := range newShards {
				if (gid != op.GID && assignedToNew < shardsPerGroup) || gid == 0 {
					newShards[shard] = op.GID
					assignedToNew++
				}
			}
			newConfig := Config{Num: len(sm.configs), Shards: newShards, Groups: newGroups}

			sm.configs = append(sm.configs, newConfig)
			sm.previous[op.GID] = true
		}
	case "Leave":
		oldConfig := sm.configs[len(sm.configs)-1]
		newGroups := make(map[int64][]string)
		newGroups[op.GID] = op.Servers
		newShards := [NShards]int64{}
		// Copy over old maps/arrays
		for gid, servers := range oldConfig.Groups {
			newGroups[gid] = servers
		}
		// Delete group
		delete(newGroups, op.GID)
		for shard, gid := range oldConfig.Shards {
			newShards[shard] = gid
		}
		var groupList []int64
		for gid, _ := range newGroups {
			groupList = append(groupList, gid)
		}
		// Group index for round robin distribution
		groupIndex := 0
		// Round robin distribute the shards that were assigned to the group that left.
		for shard, gid := range newShards {
			if gid == op.GID {
				newShards[shard] = groupList[groupIndex]
				groupIndex = (groupIndex + 1) % len(groupList)
			}
		}
		newConfig := Config{Num: len(sm.configs), Shards: newShards, Groups: newGroups}

		sm.configs = append(sm.configs, newConfig)
		sm.previous[op.GID] = true
	case "Move":
	case "Query":
		if op.Num > 0 && op.Num < len(sm.configs) {
			sm.requests[op.ID] = sm.configs[op.Num]
		} else {
			sm.requests[op.ID] = sm.configs[len(sm.configs)-1]
		}
	}
}
