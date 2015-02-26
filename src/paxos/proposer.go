package paxos

import "fmt"

import "time"
import "math/rand"

//TODO: Major Refactoring.
func (px *Paxos) Propose(proposer *Proposer) {
	fmt.Print("")
	// If fails at any step in chain, will increment proposal number and try again.
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	counter := 0
	for !proposer.Decided && !px.dead {

		successProposing := false

		counter++
		proposer.Proposal.Num = counter*len(px.peers) + px.me
		// fmt.Println("PROPOSING", proposer.Proposal.Num, px.me)
		currentProp := proposer.Proposal
		numSuccess := 0

		donePrepareChans := make([]chan *PrepareReply, len(px.peers))
		for pIndex, peer := range px.peers {
			prepareChan := make(chan *PrepareReply)
			donePrepareChans[pIndex] = prepareChan
			go px.PreparePeer(peer, proposer, prepareChan)
		}

		// Block and wait for all calls to finish
		highestAcceptNum := 0
		for _, channel := range donePrepareChans {
			reply := <-channel
			if reply != nil {
				numSuccess++
				// fmt.Println("highest accept", reply.Acceptor.HighestAccept, currentProp)
				// Set highest Value from Accepted values
				// If failed, currentProp will be reset next cycle
				if reply.Acceptor.HighestAccept.Num > highestAcceptNum {
					currentProp.Value = reply.Acceptor.HighestAccept.Value
					highestAcceptNum = reply.Acceptor.HighestAccept.Num
				}
			}
		}
		if numSuccess > len(px.peers)/2 {
			successProposing = true
		}
		// fmt.Println(doneProposing, "DONE")

		// if true, succeeded, if false, failed
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
							reply := &DecideReply{}
							args := &DecideArgs{currentProp, px.done}
							px.Decide(args, reply)
						}()
					} else {
						go func(peer string) {
							reply := &DecideReply{}
							args := &DecideArgs{currentProp, px.done}
							success := call(peer, "Paxos.Decide", args, reply)
							if success {
								// fmt.Println("SUCCESS ISSUING DECISION", currentProp.Seq, reply.Done, px.me)
								// fmt.Println("UPDATING DONE TABLE", reply.Done, px.me)
								//Update local done map.
								for server, seq := range reply.Done {
									if px.done[server] <= seq {
										px.done[server] = seq
									}
								}
							}
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
		sleep := time.Millisecond * time.Duration(r.Intn(500))
		time.Sleep(sleep)
	}
}
