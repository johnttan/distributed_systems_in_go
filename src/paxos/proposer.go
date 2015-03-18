package paxos

import "time"
import "math/rand"

func (px *Paxos) Propose(seq int, value interface{}) {
	// If fails at any step in chain, will increment proposal number and try again.
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	decided, _ := px.Status(seq)
	for !decided {
		prepare_num := int(time.Now().UnixNano())*len(px.peers) + px.me
		// fmt.Println("PROPOSING", proposer.Proposal.Num, px.me)
		numSuccess := 0
		highestAcceptNum := 0
		highestAcceptValue := value

		// donePrepareChans := make([]chan *PrepareReply, len(px.peers))
		for pIndex, peer := range px.peers {
			// prepareChan := make(chan *PrepareReply)
			// donePrepareChans[pIndex] = prepareChan
			reply := px.PreparePeer(peer, &PrepareArgs{PrepareNum: prepare_num, Seq: seq, PrepareValue: value})
			if px.done[pIndex] < reply.Done {
				px.done[pIndex] = reply.Done
			}
			if reply.Response == PREPARE_OK {
				numSuccess++
				if reply.HighestAcceptNum > highestAcceptNum {
					highestAcceptNum = reply.HighestAcceptNum
					highestAcceptValue = reply.HighestAcceptValue
				}
			}
		}

		// Block and wait for all calls to finish
		// for pIndex, channel := range donePrepareChans {
		// 	reply := <-channel
		// 	px.done[pIndex] = reply.Done
		// 	if reply.Response == PREPARE_OK {
		// 		numSuccess++
		// 		if reply.HighestAcceptNum > highestAcceptNum {
		// 			highestAcceptNum = reply.HighestAcceptNum
		// 			highestAcceptValue = reply.HighestAcceptValue
		// 		}
		// 	}
		// }
		if numSuccess < len(px.peers)/2+1 {
			sleep := time.Millisecond * time.Duration(r.Intn(1000))
			time.Sleep(sleep + 500*time.Millisecond)
			continue
		}
		// fmt.Println("DONE PROPOSING")

		// if true, succeeded, if false, failed

		numSuccess = 0
		// doneAcceptChans := make([]chan *AcceptReply, len(px.peers))
		for pIndex, peer := range px.peers {
			// acceptChan := make(chan *AcceptReply)
			// doneAcceptChans[pIndex] = acceptChan

			reply := px.AcceptPeer(peer, &AcceptArgs{AcceptNum: prepare_num, AcceptValue: highestAcceptValue, Seq: seq})
			if px.done[pIndex] < reply.Done {
				px.done[pIndex] = reply.Done
			}
			if reply.Response == ACCEPT_OK {
				numSuccess++
			}
		}

		// for _, channel := range doneAcceptChans {
		// 	reply := <-channel
		// }

		if numSuccess < len(px.peers)/2+1 {
			sleep := time.Millisecond * time.Duration(r.Intn(1000))
			time.Sleep(sleep + 500*time.Millisecond)
			continue
		}

		decided = true

		for pIndex, peer := range px.peers {
			args := &DecideArgs{DecideValue: highestAcceptValue, Done: px.done, DecideNum: highestAcceptNum, Seq: seq}

			reply := &DecideReply{}
			if px.peers[px.me] == peer {
				px.Decide(args, reply)
			} else {
				call(peer, "Paxos.Decide", args, reply)
			}
			if px.done[pIndex] < reply.Done {
				px.done[pIndex] = reply.Done
			}
		}
		// PR("log, %+v, v", px.log[seq], seq)
	}
}
