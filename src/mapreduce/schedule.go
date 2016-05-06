package mapreduce

import "fmt"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	task_chan := make(chan DoTaskArgs, ntasks)
	defer close(task_chan)

	// idle_workers := make(chan string, 1)
	// defer close(idle_workers)

	done_jobs := make(chan bool, 1)
	defer close(done_jobs)

	done_task := make(chan bool, 1)
	defer close(done_task)

	debug("ready to go\n")

	go func () {
		for i:=0; i < ntasks; i++ {
			args := DoTaskArgs{JobName:mr.jobName, File:mr.files[i], Phase:phase, TaskNumber:i, NumOtherPhase:nios}
			task_chan <- args
		}
	}()

	go func() {
		for i:=0; i < ntasks; i++ {
			<- done_jobs
			debug("%d %s jobs done\n", i+1, phase)
		}
		done_task <- true
		debug("task done signal on the way\n")
	} ()

	// go func() {
		// for {
				// worker, ok := <- mr.registerChannel
				// if ok {
					// mr.idleWorkers <- worker
				// } 
		// }
	// } ()


	done := false
	for !done {
		select {
			case done = <- done_task:
				break
			// default:
				// for args := range task_chan {
			case args := <- task_chan:
					go func() {
						// worker, ok := <- mr.idleWorkers
						LOOP:
						for {
							select {
								case worker := <- mr.registerChannel:
									debug("start rpc call\n")
									ok := call(worker, "Worker.DoTask", args, new(struct{}))
									if ok {
										debug("rpc call success\n")
										// mr.idleWorkers <- worker
										go func() {
											mr.registerChannel <- worker
											debug("send back worker: %s\n", phase)
										} ()
										done_jobs <- true
										break LOOP
									} else {
										debug("rpc call fail, send back args\n")
									}
							}
						}
					} ()
				}
		// }
	}


	// <- done_task

	fmt.Printf("Schedule: %v phase done\n", phase)
}
