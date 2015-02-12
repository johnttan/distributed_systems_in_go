package mapreduce

import "container/list"
import "fmt"


type WorkerInfo struct {
	address string
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
			mr.Workers[worker] = &WorkerInfo{address: worker}
			fmt.Println("ASSIGNING WORKERS", mr.Workers)
		}
	}()

	done := 0

	for i := range mr.nMap {
		go func() {
			mr.DoMap(i, mr.file)
			done = done + 1
			fmt.Println(done)
		}()
	}

	test := <- mr.DoneChannel
	fmt.Println(test)
	return mr.KillWorkers()
}
