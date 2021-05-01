package main

import (
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/jerheff/go-joblevel"
)

func main() {

	// Make a series of random jobs occurring every 5 to 360 minutes
	n := 10000
	jobs := joblevel.Jobs{}

	for i := 0; i < n; i++ {
		jobs = append(jobs,
			joblevel.Job{
				ID:        strconv.Itoa(i),
				Frequency: time.Minute * time.Duration(5+rand.Intn(360-5))})
	}

	log.Printf("Jobs 1: %v", jobs[1])

	// Schedule them randomly throughout the day
	start := time.Now()
	jobs.ScheduleJobs()
	elapsed := time.Since(start)

	log.Printf("Scheduling %v jobs took %v", n, elapsed)

	log.Printf("Jobs 1: %v", jobs[1])

	// Determine which jobs should be kicked off during a time period
	now := time.Now()
	duration := time.Minute
	startIDs := jobs.StartingBetween(now, now.Add(duration)).IDs()

	log.Printf("%v jobs starting in next %v: %v", len(startIDs), duration, startIDs)

}
