package paxos

func (px *Paxos) PreparePeer(peer string, proposer *Proposer, done chan *PrepareReply) {
	reply := &PrepareReply{}
	success := false
	// Run local function when calling self RPC
	if peer == px.peers[px.me] {
		err := px.Prepare(&proposer.Proposal, reply)
		if err == nil {
			success = true
		}
	} else {
		success = call(peer, "Paxos.Prepare", &proposer.Proposal, reply)
	}
	if success {
		done <- reply
		return
	}
	done <- nil
}

func (px *Paxos) AcceptPeer(peer string, proposer *Proposer, currentProp Proposal, done chan *AcceptReply) {
	reply := &AcceptReply{currentProp}
	success := false
	if peer == px.peers[px.me] {
		err := px.Accept(&currentProp, reply)
		if err == nil {
			success = true
		}
	} else {
		success = call(peer, "Paxos.Accept", currentProp, reply)
	}

	if success {
		done <- reply
		return
	}
	done <- nil
}
