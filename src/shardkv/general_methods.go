package shardkv

import "time"

func (kv *ShardKV) getLog(seq int) Op {
	to := 100 * time.Millisecond
	for {
		status, untypedOp := kv.px.Status(seq)
		if status {
			op := untypedOp.(Op)
			return op
		}
		time.Sleep(to)
	}
}

func (kv *ShardKV) logOp(newOp Op) (string, Err) {
	// Keep trying new sequence slots until successfully committed to log.
	seq := kv.latestSeq + 1
	kv.px.Start(seq, newOp)

	for {
		op := kv.getLog(seq)
		// Let paxos know we're done with this op/seq
		kv.px.Done(seq)
		kv.latestSeq = seq
		// Check if operation has been cached or is invalid because of wrong group
		_, err := kv.validateOp(op)
		// This validation is to see if op has been invalidated by previous ops

		value, oldE := kv.validateOp(newOp)
		// If op we are trying to commit is not valid, return value,oldE
		if oldE != "" {
			return value, oldE
		}

		// If the current op from log is valid, then commit it
		if err == "" {
			kv.commit(op)
		}
		// DPrintf(kv.gid, "retrying logOp, got=%+v", op)
		if op.UID == newOp.UID {
			// Return the cached version, and OK
			return kv.cache[op.ClientID], OK
		} else {
			seq += 1
			time.Sleep(100 * time.Millisecond)
			kv.px.Start(seq, newOp)
		}
	}
}

func (kv *ShardKV) tryOp(op Op) (string, Err) {
	op.UID = nrand()
	value, err := kv.logOp(op)
	return value, err
}
