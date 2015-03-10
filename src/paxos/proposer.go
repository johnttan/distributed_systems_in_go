package paxos

import "fmt"

import "time"
import "math/rand"

func (px *Paxos) Propose(proposer *Proposer) {
	fmt.Print("")
	// If fails at any step in chain, will increment proposal number and try again.
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	counter := 0
	for !proposer.Decided {
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
			if reply.Response == PREPARE_OK {
				numSuccess++
				if reply.Acceptor.HighestAccept.Num > highestAcceptNum {
					currentProp.Value = reply.Acceptor.HighestAccept.Value
					highestAcceptNum = reply.Acceptor.HighestAccept.Num
				}
			}
		}
		if numSuccess < len(px.peers)/2+1 {
			sleep := time.Millisecond * time.Duration(r.Intn(1000))
			time.Sleep(sleep + 500*time.Millisecond)
			continue
		}
		// fmt.Println(doneProposing, "DONE")

		// if true, succeeded, if false, failed

		numSuccess = 0
		doneAcceptChans := make([]chan *AcceptReply, len(px.peers))
		for pIndex, peer := range px.peers {
			acceptChan := make(chan *AcceptReply)
			doneAcceptChans[pIndex] = acceptChan
			go px.AcceptPeer(peer, proposer, currentProp, acceptChan)
		}

		for _, channel := range doneAcceptChans {
			reply := <-channel
			if reply != nil {
				numSuccess++
			}
		}

		if numSuccess < len(px.peers)/2+1 {
			sleep := time.Millisecond * time.Duration(r.Intn(1000))
			time.Sleep(sleep + 500*time.Millisecond)
			continue
		}
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
						//Update local done map.
						for server, seq := range reply.Done {
							if px.done[server] <= seq {
								px.done[server] = seq
							}
						}
					}
				}(peer)

			}
			proposer.Decided = true
			proposer.Proposal = currentProp
		}
	}
}
