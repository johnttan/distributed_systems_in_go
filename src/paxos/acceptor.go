package paxos

import "errors"

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	if px.dead {
		return errors.New("dead")
	}
	// fmt.Println("DECIDED", "SEQ", args.Prop.Seq, "Num", args.Prop.Num, "Id", args.Prop.Id, args.Done, px.me)
	px.log[args.Prop.Seq].HighestAcceptValue = args.Prop.Value
	for server, seq := range args.Done {
		//Check if it's less than or equal to existing done min. Set update local done map.
		if px.done[server] <= seq {
			px.done[server] = seq
		}
	}

	// Cleanup old map entries
	min := px.Min()

	for id, _ := range px.log {
		if id < min {
			delete(px.log, id)
		}
	}

	reply.Done = px.done
	return nil
}

func (px *Paxos) Accept(prop *Proposal, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	// If proposed num is greater than or equal to highest prepare seen, accept it.

	reply.Instance = px.log[prop.Seq]
	if prop.Seq >= px.Min() && prop.Num >= px.log[prop.Seq].HighestPrepareNum {
		px.log[prop.Seq].HighestPrepareNum = prop.Num
		px.log[prop.Seq].HighestAcceptNum = prop.Num
		px.log[prop.Seq].HighestAcceptValue = prop.Value
		px.log[prop.Seq].HighestPrepareValue = prop.Value
		reply.Response = ACCEPT_OK
		return nil
	} else {
		return errors.New("Not latest prepare")
	}
	return nil
}

func (px *Paxos) Prepare(prop *Proposal, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	reply.Instance = px.log[prop.Seq]
	// If proposed num > highest prepare seen, accept this prepare
	if px.log[prop.Seq].HighestPrepareNum < prop.Num {
		px.log[prop.Seq].HighestPrepareNum = prop.Num
		px.log[prop.Seq].HighestPrepareValue = prop.Value
		reply.Response = PREPARE_OK
		return nil
	} else {
		reply.Response = PREPARE_REJECT
		return errors.New("Old prepare")
	}
	px.newInstance(prop.Seq, prop.Num, prop.Value)
	reply.Response = PREPARE_OK
	return nil
}
