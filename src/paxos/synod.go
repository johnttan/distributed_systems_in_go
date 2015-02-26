package paxos

import "fmt"
import "errors"

func (px *Paxos) Propose(proposer *Proposer) {
	for !proposer.Decided {

		proposer.Proposal.Num++

		currentProp := proposer.Proposal
		numSuccess := 0
		numDone := 0
		doneProposing := make(chan bool)
		for _, peer := range px.peers {
			go func(peer string) {
				reply := &PrepareReply{}
				var success bool
				if peer == px.peers[px.me] {
					err := px.Prepare(&proposer.Proposal, reply)
					if err != nil {
						success = false
					} else {
						success = true
					}
				} else {
					success = call(peer, "Paxos.Prepare", &proposer.Proposal, reply)
				}
				numDone++
				if success {
					numSuccess++
					fmt.Println("highest accept", reply.Acceptor.HighestAccept, currentProp)
					if reply.Acceptor.HighestAccept.Num > currentProp.Num {
						currentProp = reply.Acceptor.HighestAccept
					}
				}

				// majority success
				if numSuccess > len(px.peers)/2 {
					doneProposing <- true
					return
				}
				// done without majority
				if numDone == len(px.peers) {
					doneProposing <- false
					return
				}
			}(peer)
		}
		// if true, succeeded, if false, failed
		successProposing := <-doneProposing

		if successProposing {
			numSuccess := 0
			numDone := 0
			doneAccepting := make(chan bool)
			for _, peer := range px.peers {
				go func(peer string) {
					reply := &AcceptReply{currentProp}
					var success bool
					if peer == px.peers[px.me] {
						err := px.Accept(&currentProp, reply)
						if err != nil {
							success = false
						} else {
							success = true
						}
					} else {
						success = call(peer, "Paxos.Accept", currentProp, reply)

					}
					numDone++
					if success {
						numSuccess++
					}

					// majority success
					if numSuccess > len(px.peers)/2 {
						doneAccepting <- true
						return
					}
					// done without majority
					if numDone == len(px.peers) {
						doneAccepting <- false
						return
					}
				}(peer)
			}

			successAccepting := <-doneAccepting

			if successAccepting {
				for _, peer := range px.peers {
					if px.peers[px.me] == peer {
						go func() {
							reply := &AcceptReply{}
							px.Decide(&currentProp, reply)
						}()
					} else {
						go func(peer string) {
							reply := &AcceptReply{}
							call(peer, "Paxos.Decide", currentProp, reply)
						}(peer)

					}
				}
				proposer.Decided = true
				if currentProp.Seq > px.highestKnown {
					px.highestKnown = currentProp.Seq
				}
				proposer.Proposal = currentProp
			} else {
			}
		} else {
		}

	}
}

func (px *Paxos) Decide(prop *Proposal, reply *AcceptReply) error {
	fmt.Println("DECIDED", prop, px.peers[px.me], px.peers)
	px.log[prop.Seq] = prop.Value
	return nil
}

func (px *Paxos) Accept(prop *Proposal, reply *AcceptReply) error {
	reply.Prop = *prop
	if prop.Num >= px.acceptors[prop.Seq].HighestPrepare.Num {
		px.acceptors[prop.Seq].HighestPrepare = *prop
		px.acceptors[prop.Seq].HighestAccept = *prop
		px.acceptors[prop.Seq].Decided = true
		if prop.Seq > px.highestKnown {
			px.highestKnown = prop.Seq
		}
		return nil
	} else {
		return errors.New("Not latest prepare")
	}
}

func (px *Paxos) Prepare(prop *Proposal, reply *PrepareReply) error {
	if _, ok := px.acceptors[prop.Seq]; ok {
		reply.Acceptor = *px.acceptors[prop.Seq]
		if px.acceptors[prop.Seq].HighestPrepare.Num < prop.Num {
			px.acceptors[prop.Seq].HighestPrepare = *prop
			return nil
		} else {
			return errors.New("Old prepare")
		}
	} else {
		px.acceptors[prop.Seq] = new(Acceptor)
		px.acceptors[prop.Seq].Seq = prop.Seq
		px.acceptors[prop.Seq].HighestPrepare = *prop
		reply.Acceptor = *px.acceptors[prop.Seq]
		return nil
	}
}
