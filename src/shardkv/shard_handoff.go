package shardkv

import "shardmaster"

func (kv *ShardKV) ReceiveShard(args *SendShardArgs, reply *SendShardReply) error {
	newOp := Op{
		Op:             "ReceiveShard",
		MigrationReply: args.MigrationReply,
		Shard:          args.MigrationReply.Shard,
		Config:         args.MigrationReply.Config,
	}
	_, err := kv.tryOp(newOp)
	reply.Err = err
	return nil
}

func (kv *ShardKV) SendShard(gid int64, payload *RequestKVReply) bool {
	done := false

	for _, srv := range kv.newConfig.Groups[gid] {
		args := &SendShardArgs{MigrationReply: payload}
		reply := &SendShardReply{}
		DPrintf(kv.gid, "calling receiveshard %+v", args.MigrationReply)

		ok := call(srv, "ShardKV.ReceiveShard", args, reply)

		if ok {
			DPrintf(kv.gid, "receive shard reply %+v", reply)

			if reply.Err == ErrWrongGroup || reply.Err == OK {
				done = true
				kv.shardsToSend[payload.Shard] = false
				break
			}
		}
	}
	return done
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

		done := false
		for _, sendDone := range kv.shardsToSend {
			done = done || sendDone
		}
		for _, receiveDone := range kv.shardsToReceive {
			done = done || receiveDone
		}
		DPrintf(kv.gid, "reconfiguring, sendDone=%+v, receiveDone=%+v, config=%+v", kv.shardsToSend, kv.shardsToReceive, config)
		if done {
			// DPrintf(kv.gid, "reconfigurecheck failed %v", nrand())
			return false
		} else {
			doneOp := Op{Op: "StopConfig", Config: *config}
			kv.tryOp(doneOp)
			return true
		}
	} else {

		startOp := Op{Op: "StartConfig", Config: *config}
		kv.tryOp(startOp)
		for shard, todo := range kv.shardsToSend {
			if todo {
				sendOp := Op{Op: "SendShard", Shard: shard, Config: *config}
				kv.tryOp(sendOp)
			}
		}
		return false
	}
}
