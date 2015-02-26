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
	} else {
		done <- nil
	}
}

// if success {
// numSuccess++
// // fmt.Println("highest accept", reply.Acceptor.HighestAccept, currentProp)
// // Set highest Value from Accepted values
// // If failed, currentProp will be reset next cycle
// if reply.Acceptor.HighestAccept.Num > highestAcceptNum {
//   currentProp.Value = reply.Acceptor.HighestAccept.Value
//   highestAcceptNum = reply.Acceptor.HighestAccept.Num
// }
// }
