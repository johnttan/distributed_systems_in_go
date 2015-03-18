package shardmaster

// TODO: REFACTOR COMMON FUNCTIONALITY i.e. copying and creating new configs, etc.
// This commit method is core of state machine.
// This is the only method that mutates core state.
func (sm *ShardMaster) Commit(op Op) {
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
			countTable := make(map[int64]int)

			for shard, gid := range oldConfig.Shards {
				newShards[shard] = gid
				countTable[gid]++
			}

			// Find # of shards per group needed to be balanced
			shardsPerGroup := NShards / len(newGroups)
			if shardsPerGroup == 0 {
				shardsPerGroup = 1
			}
			keys := []int64{}
			for key, _ := range newGroups {
				keys = append(keys, key)
			}

			for newgid, _ := range newGroups {
				count := countTable[newgid]
				// Keep track of how many reassigned to new group
				if count < shardsPerGroup+1 {
					for shard, gid := range newShards {
						if (gid != newgid && countTable[newgid] < shardsPerGroup && countTable[gid] > shardsPerGroup) || gid == 0 {
							newShards[shard] = newgid
							countTable[gid]--
							countTable[newgid]++
						}
					}
				}
			}

			// DPrintf("JOINING, keys= %+v, \n %+v \n op= %+v, counts= %+v, pergroup= %+v", keys, newShards, op, countTable, shardsPerGroup)
			// Find shards not assigned to new group, and if new group still needs shards, assign current shard to new group

			newConfig := Config{Num: len(sm.configs), Shards: newShards, Groups: newGroups}

			sm.configs = append(sm.configs, newConfig)
			sm.previous[op.GID] = true
		}
	case "Leave":
		oldConfig := sm.configs[len(sm.configs)-1]
		newGroups := make(map[int64][]string)
		newGroups[op.GID] = op.Servers
		newShards := [NShards]int64{}

		groupList := []int64{}
		// Copy over old maps/arrays
		for gid, servers := range oldConfig.Groups {
			newGroups[gid] = servers
			if gid != op.GID {
				groupList = append(groupList, gid)
			}
		}
		// Delete group
		delete(newGroups, op.GID)
		delete(sm.previous, op.GID)
		countTable := make(map[int64]int)

		for shard, gid := range oldConfig.Shards {
			newShards[shard] = gid
			countTable[gid]++
		}
		delete(countTable, op.GID)
		// Find # of shards per group needed to be balanced
		shardsPerGroup := NShards / len(newGroups)
		if shardsPerGroup == 0 {
			shardsPerGroup = 1
		}
		// Find shards not assigned to new group, and if new group still needs shards, assign current shard to new group

		for shard, gid := range newShards {
			if gid == op.GID {
				for group, count := range countTable {
					if count < shardsPerGroup {
						newShards[shard] = group
						countTable[group]++
					}
				}
			}
		}
		// start round robin to distribute rest
		groupIndex := 0
		for shard, gid := range newShards {
			if gid == op.GID {
				newShards[shard] = groupList[groupIndex]
				groupIndex = (groupIndex + 1) % len(groupList)
			}
		}
		countTable = make(map[int64]int)

		for shard, gid := range newShards {
			newShards[shard] = gid
			countTable[gid]++
		}
		for newgid, _ := range newGroups {
			count := countTable[newgid]
			// Keep track of how many reassigned to new group
			if count < shardsPerGroup+1 {
				for shard, gid := range newShards {
					if (gid != newgid && countTable[newgid] < shardsPerGroup && countTable[gid] > shardsPerGroup) || gid == 0 {
						newShards[shard] = newgid
						countTable[gid]--
						countTable[newgid]++
					}
				}
			}
		}

		newConfig := Config{Num: len(sm.configs), Shards: newShards, Groups: newGroups}
		sm.configs = append(sm.configs, newConfig)
		keys := []int64{}
		for key, _ := range newConfig.Groups {
			keys = append(keys, key)
		}
		// DPrintf("LEAVING, groups= %+v, \n countTable= %+v,\n gid leaving= %v,\n shards= %+v,\n pergroup=%v, groups=%v, shards=%v", keys, countTable, op.GID, newShards, shardsPerGroup, len(newGroups), NShards)
		delete(sm.previous, op.GID)
	case "Move":
		oldConfig := sm.configs[len(sm.configs)-1]
		newGroups := make(map[int64][]string)
		newGroups[op.GID] = op.Servers
		newShards := [NShards]int64{}
		// Copy over old maps/arrays
		for gid, servers := range oldConfig.Groups {
			newGroups[gid] = servers
		}
		for shard, gid := range oldConfig.Shards {
			if shard == op.Shard {
				newShards[shard] = op.GID
			} else {
				newShards[shard] = gid
			}
		}

		newConfig := Config{Num: len(sm.configs), Shards: newShards, Groups: newGroups}

		sm.configs = append(sm.configs, newConfig)
	case "Query":
		if op.Num > -1 && op.Num < len(sm.configs) {
			sm.requests[op.ID] = sm.configs[op.Num]
		} else {
			sm.requests[op.ID] = sm.configs[len(sm.configs)-1]
		}
	}
}
