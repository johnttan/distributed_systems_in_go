package paxos

import "fmt"
import "errors"
import "time"
import "math/rand"

//TODO: Major Refactoring.
func (px *Paxos) Propose(proposer *Proposer) {
	// If fails at any step in chain, will increment proposal number and try again.
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	for !proposer.Decided {

		proposer.Proposal.Num++

		currentProp := proposer.Proposal
		numSuccess := 0
		numDone := 0
		doneProposing := make(chan bool)
		highestAcceptNum := 0
		for _, peer := range px.peers {
			go func(peer string) {
				reply := &PrepareReply{}
				var success bool
				// Run local function when calling self RPC
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
					// fmt.Println("highest accept", reply.Acceptor.HighestAccept, currentProp)
					// Set highest Value from Accepted values
					// If failed, currentProp will be reset next cycle
					if reply.Acceptor.HighestAccept.Num > highestAcceptNum {
						currentProp.Value = reply.Acceptor.HighestAccept.Value
						highestAcceptNum = reply.Acceptor.HighestAccept.Num
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
								fmt.Println("UPDATING DONE TABLE", reply.Done, px.me)
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
		sleep := time.Millisecond * time.Duration(r.Intn(100))
		time.Sleep(sleep)
	}
}

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
	fmt.Println("DECIDED", args.Done, px.me)
	px.log[args.Prop.Seq] = args.Prop.Value
  for server, seq := range args.Done {
    //Check if it's less than or equal to existing done min. Set update local done map.
    if px.done[server] <= seq {
      px.done[server] = seq
    }
  }
	reply.Done = px.done
	return nil
}

func (px *Paxos) Accept(prop *Proposal, reply *AcceptReply) error {
	px.mu.Lock()
	reply.Prop = *prop
	// If proposed num is greater than or equal to highest prepare seen, accept it.
	if prop.Num >= px.acceptors[prop.Seq].HighestPrepare.Num {
		px.acceptors[prop.Seq].HighestPrepare = *prop
		px.acceptors[prop.Seq].HighestAccept = *prop
		px.acceptors[prop.Seq].Decided = true
		if prop.Seq > px.highestKnown {
			px.highestKnown = prop.Seq
		}
		px.mu.Unlock()
		return nil
	} else {
		px.mu.Lock()
		return errors.New("Not latest prepare")
	}
}

func (px *Paxos) Prepare(prop *Proposal, reply *PrepareReply) error {
	px.mu.Lock()
	if _, ok := px.acceptors[prop.Seq]; ok {
		reply.Acceptor = *px.acceptors[prop.Seq]
		// If proposed num > highest prepare seen, accept this prepare
		if px.acceptors[prop.Seq].HighestPrepare.Num < prop.Num {
			px.acceptors[prop.Seq].HighestPrepare = *prop
			px.mu.Unlock()
			return nil
		} else {
			px.mu.Unlock()
			return errors.New("Old prepare")
		}
	} else {
		px.acceptors[prop.Seq] = new(Acceptor)
		px.acceptors[prop.Seq].Seq = prop.Seq
		px.acceptors[prop.Seq].HighestPrepare = *prop
		reply.Acceptor = *px.acceptors[prop.Seq]
		px.mu.Unlock()
		return nil
	}
}
