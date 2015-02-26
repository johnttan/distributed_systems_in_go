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
				success := call(peer, "Paxos.Prepare", &proposer.Proposal, reply)
				numDone++
				if success {
					numSuccess++
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
					success := call(peer, "Paxos.Accept", currentProp, reply)
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
					go func(peer string) {
						reply := &AcceptReply{}
						call(peer, "Paxos.Decide", currentProp, reply)
					}(peer)
				}
				proposer.decided = true
				px.log[currentProp.Seq] = currentProp.Value
				proposer.Proposal = currentProp
			} else {
				fmt.Println("FAILED ACCEPTING")
			}
		} else {
			fmt.Println("FAILED PROPOSING")
		}

	}
}

func (px *Paxos) Decide(prop *Proposal, reply *AcceptReply) error {
	px.log[prop.Seq] = prop.Value
}

func (px *Paxos) Accept(prop *Proposal, reply *AcceptReply) error {
	reply.Acceptor = px.Acceptors[prop.Seq]
	if prop.Num >= px.Acceptors[prop.Seq].HighestPrepare.Num {
		px.Acceptors[prop.Seq].HighestPrepare = prop
		px.Acceptors[prop.Seq].HighestAccept = prop
		px.Acceptors[prop.Seq].Decided = true
		return nil
	} else {
		return errors.New("Not latest prepare")
	}
}

func (px *Paxos) Prepare(prop *Proposal, reply *PrepareReply) error {
	fmt.Println("prepare called", prop, px.me)
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
