package shardkv

func (kv *ShardKV) isConfiguring() bool {
	// True if configuring, else false
	if kv.config.Num == 0 || kv.config.Num == 1 {
		return false
	}
	for shard, gid := range kv.config.Shards {
		if gid == kv.gid {
			notReceived, needed := kv.shardsNeeded[shard]
			if notReceived && needed {
				return true
			}
		}
	}
	return false
}

func (kv *ShardKV) ReceiveShard(args *SendShardArgs, reply *SendShardReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	newOp := Op{
		Op:     "ReceiveShard",
		Data:   args.Data,
		Shard:  args.Shard,
		Config: args.Config,
	}
	_, err := kv.tryOp(newOp)
	reply.Err = err
	return nil
}

func (kv *ShardKV) merge(newData map[string]string, data map[string]string) {
	for key, value := range data {
		newData[key] = value
	}
}
