package shardkv

import "shardmaster"

func (kv *ShardKV) ReceiveShard(args *SendShardArgs, reply *SendShardReply) {

}

func (kv *ShardKV) SendShard(gid int64, payload *RequestKVReply) {
	for _, srv := range kv.config.Groups[gid] {
		args := &SendShardArgs{MigrationReply: payload}
		reply := &SendShardReply{}
		ok := call(srv, "KV.ReceiveShard", args, reply)
		if ok {
			kv.shardsToSend[payload.Shard] = false
			break
		}
	}
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

func (kv *ShardKV) merge(newReq map[int64]int64, newCache map[int64]string, newData map[string]string, reply *RequestKVReply) {
	for clientID, reqID := range reply.Requests {
		oldReqID, ok := newReq[clientID]
		if !ok || oldReqID < reqID {
			newReq[clientID] = reqID
			newCache[clientID] = reply.Cache[clientID]
		}
	}

	for key, value := range reply.Data {
		newData[key] = value
	}
}

func (kv *ShardKV) reconfigure(config *shardmaster.Config) bool {
	currentConfig := kv.config

	newRequests := make(map[int64]int64)
	newCache := make(map[int64]string)
	newData := make(map[string]string)
	// Find all shards/caches and merge them into latest maps
	for shard := 0; shard < len(config.Shards); shard++ {
		// If new shard
		// DPrintf(kv.gid, "oldshards = %+v, new shards = %+v", currentConfig, config)
		if config.Shards[shard] == kv.gid && currentConfig.Shards[shard] != kv.gid {
			servers := currentConfig.Groups[currentConfig.Shards[shard]]
			foundShard := false
			if len(servers) == 0 {
				foundShard = true
			}
			for _, srv := range servers {
				args := &RequestKVArgs{}
				args.Shard = shard
				args.Config = currentConfig

				reply := &RequestKVReply{}

				ok := call(srv, "ShardKV.GetShard", args, reply)
				if ok && (reply.Err == ErrWrongGroup) {
					return false
				}
				if ok {
					kv.merge(newRequests, newCache, newData, reply)
					foundShard = true
					break
				}
				// DPrintf(kv.gid, "starting reconfigure merge %+v", reply.Data)
			}
			if !foundShard {
				return false
			}
		}
	}

	newOp := Op{
		Op:             "Config",
		MigrationReply: &RequestKVReply{Cache: newCache, Requests: newRequests, Data: newData},
		Config:         *config,
	}
	kv.tryOp(newOp)
	// log.Printf("RECONFIGURING %+v \n\n", newOp.MigrationReply)
	return true
}
