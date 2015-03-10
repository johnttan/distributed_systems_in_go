package paxos

import "errors"

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	if px.dead {
		return errors.New("dead")
	}
	// fmt.Println("DECIDED", "SEQ", args.Prop.Seq, "Num", args.Prop.Num, "Id", args.Prop.Id, args.Done, px.me)
	px.log[args.Prop.Seq] = args.Prop.Value
	for server, seq := range args.Done {
		//Check if it's less than or equal to existing done min. Set update local done map.
		if px.done[server] <= seq {
			px.done[server] = seq
		}
	}

	// Cleanup old map entries
	min := px.Min()

	for id, _ := range px.acceptors {
		if id < min {
			// delete(px.acceptors, id)
			px.acceptors[id] = nil
		}
	}

	for id, _ := range px.log {
		if id < min {
			// delete(px.log, id)
			px.log[id] = nil
		}
	}

	reply.Done = px.done
	return nil
}

func (px *Paxos) Accept(prop *Proposal, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	reply.Prop = *prop
	// If proposed num is greater than or equal to highest prepare seen, accept it.

	if _, ok := px.acceptors[prop.Seq]; ok && prop.Seq >= px.Min() {
		if prop.Num >= px.acceptors[prop.Seq].HighestPrepare.Num {
			px.acceptors[prop.Seq].HighestPrepare = *prop
			px.acceptors[prop.Seq].HighestAccept = *prop
			px.acceptors[prop.Seq].Decided = true
			return nil
		} else {
			return errors.New("Not latest prepare")
		}
	} else {
		px.acceptors[prop.Seq] = new(Acceptor)
		px.acceptors[prop.Seq].Seq = prop.Seq
		px.acceptors[prop.Seq].HighestPrepare = *prop
		return nil
	}
}

func (px *Paxos) Prepare(prop *Proposal, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	if _, ok := px.acceptors[prop.Seq]; ok {
		reply.Acceptor = *px.acceptors[prop.Seq]
		// If proposed num > highest prepare seen, accept this prepare
		if px.acceptors[prop.Seq].HighestPrepare.Num < prop.Num {
			px.acceptors[prop.Seq].HighestPrepare = *prop
			reply.Response = PREPARE_OK
			return nil
		} else {
			reply.Response = PREPARE_REJECT
			return errors.New("Old prepare")
		}
	} else {
		px.acceptors[prop.Seq] = new(Acceptor)
		px.acceptors[prop.Seq].Seq = prop.Seq
		px.acceptors[prop.Seq].HighestPrepare = *prop
		reply.Acceptor = *px.acceptors[prop.Seq]
		reply.Response = PREPARE_OK
		return nil
	}
}
