package main

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

// Task represents a ride request.
type Task struct {
	ID          int
	Passenger   string
	Pickup      string
	Destination string
	IsPoison    bool // special "poison pill" to tell workers to exit
}

// RideResult holds details of a completed ride.
type RideResult struct {
	TaskID       int
	Passenger    string
	Pickup       string
	Destination  string
	Driver       string
	ProcessingMs int64
	CompletedAt  time.Time
}

// Process simulates task processing.
func (t Task) Process() (RideResult, error) {
	if t.IsPoison {
		return RideResult{}, nil
	}
	// Simulate random processing delay
	duration := time.Duration(300+rand.Intn(900)) * time.Millisecond
	time.Sleep(duration)

	driver := fmt.Sprintf("Driver-%d", rand.Intn(20)+1)
	return RideResult{
		TaskID:       t.ID,
		Passenger:    t.Passenger,
		Pickup:       t.Pickup,
		Destination:  t.Destination,
		Driver:       driver,
		ProcessingMs: duration.Milliseconds(),
		CompletedAt:  time.Now(),
	}, nil
}

// Worker consumes tasks from the channel until a poison pill is received.
func Worker(id int, tasks <-chan Task, results chan<- RideResult, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("Worker-%d started", id)
	for task := range tasks {
		if task.IsPoison {
			log.Printf("Worker-%d received poison pill, exiting", id)
			return
		}
		log.Printf("Worker-%d processing Task-%d (%s)", id, task.ID, task.Passenger)
		res, err := task.Process()
		if err != nil {
			log.Printf("Worker-%d error processing Task-%d: %v", id, task.ID, err)
			continue
		}
		log.Printf("Worker-%d completed Task-%d -> %s", id, res.TaskID, res.Driver)
		results <- res
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	numWorkers := 4
	numTasks := 15

	tasks := make(chan Task, numTasks+numWorkers)
	results := make(chan RideResult, numTasks)

	var wg sync.WaitGroup

	// Start workers
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go Worker(i, tasks, results, &wg)
	}

	// Enqueue tasks
	for i := 1; i <= numTasks; i++ {
		t := Task{
			ID:          i,
			Passenger:   fmt.Sprintf("Passenger-%d", i),
			Pickup:      fmt.Sprintf("Pickup-%d", (i%5)+1),
			Destination: fmt.Sprintf("Dest-%d", (i%4)+1),
		}
		tasks <- t
		log.Printf("Main enqueued Task-%d for %s", t.ID, t.Passenger)
	}

	// Send poison pills (one per worker)
	for i := 0; i < numWorkers; i++ {
		tasks <- Task{IsPoison: true}
	}

	close(tasks)

	// Collect results in a separate goroutine
	go func() {
		wg.Wait()    // Wait for all workers to finish
		close(results) // Then close results channel
	}()

	// Output final results
	fmt.Println("\n--- Ride Results ---")
	for r := range results {
		fmt.Printf("Task[%d] %s from %s to %s | Driver=%s | %dms | %s\n",
			r.TaskID, r.Passenger, r.Pickup, r.Destination, r.Driver,
			r.ProcessingMs, r.CompletedAt.Format(time.RFC3339))
	}
	fmt.Println("--- Simulation Complete ---")
}
