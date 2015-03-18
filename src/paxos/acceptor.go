package paxos

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	px.newInstance(args.Seq, "Decide")
	// fmt.Println("DECIDED", "SEQ", args.inst.Seq, "Num", args.Prop.Num, "Id", args.Prop.Id, args.Done, px.me)
	px.log[args.Seq].DecidedValue = args.DecideValue
	px.log[args.Seq].Decided = true
	reply.Done = px.done[px.me]
	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	// If proposed num is greater than or equal to highest prepare seen, accept it.
	px.newInstance(args.Seq, "Accept")
	// fmt.Println("ACCEPT", px.me)
	if args.AcceptNum >= px.log[args.Seq].HighestPrepareNum {
		px.log[args.Seq].HighestPrepareNum = args.AcceptNum
		px.log[args.Seq].HighestAcceptNum = args.AcceptNum
		px.log[args.Seq].HighestAcceptValue = args.AcceptValue
		reply.Response = ACCEPT_OK
	} else {
		reply.Response = ACCEPT_REJECT
	}
	reply.Done = px.done[px.me]
	return nil
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	px.newInstance(args.Seq, "Prepare")
	// fmt.Println("PREPARE", px.me)
	// If proposed num > highest prepare seen, accept this prepare
	if px.log[args.Seq].HighestPrepareNum < args.PrepareNum {
		px.log[args.Seq].HighestPrepareNum = args.PrepareNum
		reply.Response = PREPARE_OK
		reply.HighestAcceptNum = px.log[args.Seq].HighestAcceptNum
		reply.HighestAcceptValue = px.log[args.Seq].HighestAcceptValue
		reply.Done = px.done[px.me]
	} else {
		reply.Response = PREPARE_REJECT
		reply.HighestAcceptNum = px.log[args.Seq].HighestAcceptNum
		reply.HighestAcceptValue = px.log[args.Seq].HighestAcceptValue
	}
	reply.Done = px.done[px.me]
	return nil
}
