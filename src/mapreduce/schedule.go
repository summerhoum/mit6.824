package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	var wg sync.WaitGroup

	for i := 0; i < ntasks; i++ {
		wg.Add(1)

		// Extract worker outside the goroutine for restarting task.
		// Question: why does it fail when a worker is fetched in goroutine?
		worker := <-registerChan

		// Each task is correlate with i, therefore if the task execute failed,
		// use i = i - 1 can restart the task.
		go func(v int, phase jobPhase, n_other int, worker string) {

			defer wg.Done()

			args := DoTaskArgs{
				JobName:       jobName,
				File:          mapFiles[v],
				Phase:         phase,
				TaskNumber:    v,
				NumOtherPhase: n_other,
			}

			ok := call(worker, "Worker.DoTask", args, new(struct{}))
			if !ok {
				// Restart the task(for mapper, reread mr.files[i).
				i = i - 1
				fmt.Println("call worker error!")
			} else {
				go func() {
					registerChan <- worker
				}()
			}

		}(i, phase, n_other, worker)

	}

	wg.Wait()

	fmt.Printf("Schedule: %v phase done\n", phase)
}
