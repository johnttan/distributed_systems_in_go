package paxos

func (px *Paxos) PreparePeer(peer string, args *PrepareArgs) *PrepareReply {
	reply := &PrepareReply{}
	// Run local function when calling self RPC
	if peer == px.peers[px.me] {
		px.Prepare(args, reply)
	} else {
		call(peer, "Paxos.Prepare", args, reply)
	}
	return reply
}

func (px *Paxos) AcceptPeer(peer string, args *AcceptArgs) *AcceptReply {
	reply := &AcceptReply{}
	if peer == px.peers[px.me] {
		px.Accept(args, reply)
	} else {
		call(peer, "Paxos.Accept", args, reply)
	}
	return reply
}
