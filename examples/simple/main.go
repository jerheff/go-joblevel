package main

import (
	"log"
	"strconv"
	"time"

	"github.com/jerheff/go-joblevel"
)

func main() {
	log.Print(joblevel.Message)

	jobs := make(joblevel.Jobs, 0)

	for i := 0; i < 10; i++ {
		jobs = append(jobs, joblevel.Job{ID: strconv.Itoa(i), Frequency: time.Hour * 12})
	}

	for _, job := range jobs {
		log.Printf("Job %s: %v", job.ID, job.RunsPerDay())
	}

	// starts := jobs.GetFirstStartTimes()

	// log.Print(starts)

	jobs.ScheduleJobs()

	log.Print(jobs)

	n := time.Now()

	log.Print(n)

	nT := n.Truncate(time.Hour * 24)

	log.Print(nT)

	j := jobs[0]
	log.Printf("The job: %v", j)

	start := time.Now().Truncate(time.Hour * 24).Add(time.Hour)

	end := start.Add(time.Hour)

	log.Printf("Job starts between %v and %v: %v", start, end, j.StartsBetween(start, end))

}
