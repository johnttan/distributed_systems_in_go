package mapreduce

import "container/list"
import "fmt"


type WorkerInfo struct {
	address string
	working bool
	// You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	mr.Workers = make(map[string]*WorkerInfo)
	// Your code here
	fmt.Println("NUMMAPS", mr.nMap)
	fmt.Println("NUMREDUCES", mr.nReduce)
	go func() {
		for {
			worker := <- mr.registerChannel
			mr.Workers[worker] = &WorkerInfo{address: worker, working:false}
			fmt.Println("REGISTERINGWORKERS", mr.Workers)
		}
	}()

	numDone := 0
	done := make(chan bool)

	for i := 0; i < mr.nMap; i++ {
		jobNum := i

		go func() {
			jobArgs := DoJobArgs{File: mr.file, Operation: Map, JobNumber: jobNum, NumOtherPhase: mr.nReduce}

			var workerChosen WorkerInfo;

			for len(workerChosen.address) < 1 {
				for _, v := range mr.Workers {
					if(!v.working){
						workerChosen = *v
					}
				}
			}
			reply := &DoJobReply{false}
			// fmt.Println("ASSIGNING WORK", workerChosen.address, jobArgs, DoJobReply{false})

			call(workerChosen.address, "Worker.DoJob", &jobArgs, reply)
			workerChosen.working = true
			for !reply.OK {

			}
			numDone += 1
			if numDone == mr.nMap {
				done <- true
			}
			// fmt.Println("DONE WITH", numDone)
		}()
	}

	doneWithMap := <- done

	fmt.Println(doneWithMap, "DONE WITH MAP")

	numDoneReduce := 0
	doneReduce := make(chan bool)

	for i := 0; i < mr.nReduce; i++ {
		jobNum := i

		go func() {
			jobArgs := DoJobArgs{File: mr.file, Operation: Reduce, JobNumber: jobNum, NumOtherPhase: mr.nMap}

			var workerChosen WorkerInfo;

			for len(workerChosen.address) < 1 {
				for _, v := range mr.Workers {
					if(!v.working){
						workerChosen = *v
					}
				}
			}
			reply := &DoJobReply{false}
			// fmt.Println("ASSIGNING Reduce WORK", workerChosen.address, jobArgs, DoJobReply{false})

			call(workerChosen.address, "Worker.DoJob", &jobArgs, reply)
			workerChosen.working = true
			for !reply.OK {

			}
			numDoneReduce += 1
			if numDoneReduce == mr.nReduce {
				doneReduce <- true
			}
			// fmt.Println("DONE WITH Reduce", numDoneReduce)
		}()
	}

	doneWithReduce := <- doneReduce

	fmt.Println(doneWithReduce, "DONE WITH REDUCE")

	return mr.KillWorkers()
}
