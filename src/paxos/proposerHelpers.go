package paxos

func (px *Paxos) PreparePeer(peer string, proposer *Proposer, done chan *PrepareReply) {
	reply := &PrepareReply{}
	// Run local function when calling self RPC
	if peer == px.peers[px.me] {
		px.Prepare(&proposer.Proposal, reply)
	} else {
		call(peer, "Paxos.Prepare", &proposer.Proposal, reply)
	}
	done <- reply
}

func (px *Paxos) AcceptPeer(peer string, proposer *Proposer, currentProp Proposal, done chan *AcceptReply) {
	reply := &AcceptReply{currentProp}
	if peer == px.peers[px.me] {
		px.Accept(&currentProp, reply)
	} else {
		call(peer, "Paxos.Accept", currentProp, reply)
	}
	done <- reply
}
