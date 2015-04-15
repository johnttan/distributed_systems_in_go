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

func (kv *ShardKV) getShard(args *RequestKVArgs, reply *RequestKVReply) error {
	reply.Requests = make(map[int64]int64)
	reply.Cache = make(map[int64]string)
	reply.Data = make(map[string]string)
	for key, val := range kv.data {
		if key2shard(key) == args.Shard {
			reply.Data[key] = val
		}
	}
	for client, req := range kv.requests {
		reply.Requests[client] = req
		reply.Cache[client] = kv.cache[client]
	}
	return nil
}

func (kv *ShardKV) merge(newData map[string]string, data map[string]string) {
	for key, value := range data {
		newData[key] = value
	}
}
