package paxos

import "fmt"

func (px *Paxos) Propose(prop *Proposer) {
	for !prop.decided {

		prop.proposal.num++

		currentProp := prop.proposal
		numSuccess := 0
		numDone := 0
		doneProposing := make(chan bool)
		for _, peer := range px.peers {
			go func() {
				reply := &PrepareReply{}
				success := call(peer, "Prepare", prop.proposal, reply)
				numDone++
				if success {
					numSuccess++
					if reply.acceptor.highestAccept.num > currentProp.num {
						currentProp = reply.acceptor.highestAccept
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
			}()
		}
		// if true, succeeded, if false, failed
		<-doneProposing
		successProposing := <-doneProposing
		if successProposing {
			fmt.Println("success proposing")
		}

	}
}

func (px *Paxos) Prepare(prop *Proposal) {

}
