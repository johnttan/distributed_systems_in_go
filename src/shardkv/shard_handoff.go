package shardkv

import "shardmaster"

func (kv *ShardKV) ReceiveShard(args *SendShardArgs, reply *SendShardReply) error {
  newOp := Op{
    Op:             "ReceiveShard",
    MigrationReply: args.MigrationReply,
    Shard: args.MigrationReply.Shard
  }
  _, err := kv.tryOp(newOp)
  reply.Err = err
  return nil
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
  if kv.reconfiguring {
    done := true
    for _, sendDone := range kv.shardsToSend {
      done = done && sendDone
    }
    for _, receiveDone := range kv.shardsToReceive {
      done = done && receiveDone
    }
    if !done{
      return false
    } else {
      doneOp := Op{Op: "StopConfig", Config: config}
      kv.tryOp(doneOp)
      return true
    }
  }else{
    startOp := Op{Op: "StartConfig", Config: config}
    kv.tryOp(doneOp)
    return false
  }
}
