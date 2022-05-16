package main

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func main() {
	PrintMemUsage()
	rand.Seed(time.Now().UnixNano())
	numMsgs := int64(1000000)

	// If each goroutine commits to the set directly, we'll need
	// a mutex and we'll have 10 million goroutines competing for
	// that mutex. So make a channel and do the commit single threaded.
	commitChan := make(chan int64, numMsgs)

	// create a WaitGroup so all goroutines will start running together
	waitStart := sync.WaitGroup{}
	waitStart.Add(1)
	// start a goroutine for each msg
	for i := int64(0); i < numMsgs; i++ {
		go func(offset int64) {
			waitStart.Wait()
			// sleep a random duration less than 1000ms
			time.Sleep(time.Duration(rand.Intn(1000) * int(time.Millisecond)))
			// commit the message offset to the local committer
			commitChan <- offset
		}(i)
	}
	fmt.Printf("finished creating %v goroutines\n", numMsgs)

	// committed stores the largest offset committed back to kafka
	committed := int64(-1)
	go func() {
		// the keys of a map are a set.  We don't care about map value
		commits := make(map[int64]struct{})
		for val := range commitChan {
			// add val to locally commited set
			commits[val] = struct{}{}

			// We use an atomic variable to track the sequential commits just
			// so that our main func can use it to track progress.
			c := atomic.LoadInt64(&committed)

			// iterate committed set from committed + 1, looking for
			// sequential values that can be committed
			next := c + 1
			_, ok := commits[next]
			for ok {
				c = next
				// don't keep sequentially committed values in the set
				delete(commits, next)
				next = c + 1
				_, ok = commits[next]
			}
			// here, we could commit c back to kafka as the largest
			// sequential offset already processed
			atomic.StoreInt64(&committed, c)
		}
	}()

	fmt.Printf("waking %v goroutines\n", numMsgs)
	PrintMemUsage()
	waitStart.Done()
	fmt.Printf("starting commit test\n")
	PrintMemUsage()
	start := time.Now()
	// set a ticker to check the max committed value every 250ms
	ticker := time.NewTicker(250 * time.Millisecond)
	for range ticker.C {
		c := atomic.LoadInt64(&committed)

		if c < numMsgs-1 {
			fmt.Printf("Committed %v\n", c)
			PrintMemUsage()
		} else {
			fmt.Printf("Committed %v\n", c)
			runtime.GC()
			PrintMemUsage()
			fmt.Printf("finished test in %v\n", time.Since(start))
			break
		}
	}
}
