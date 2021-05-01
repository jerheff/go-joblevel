package joblevel

import (
	"math"
	"time"

	"github.com/cespare/xxhash/v2"
)

const Message = "Helloo!"

const dayDuration = 24 * time.Hour

// determine how to scale the max Int64 (largest hash possible) to the max duration of a day
const hashToDayDurationScaler = float64(dayDuration) / math.MaxUint64

/*

Problem:
	We have a set of jobs.  Each job needs to run every X minutes.
	We want to schedule these jobs in a way that spreads the load across the day.
	When a new job is added we don't want all of the existing jobs to move to different start times of the day.

	A run task scheduler kicks off on a schedule (say every 5 minutes) and needs to determine which jobs to run.

Structures:
	Job
		Identifier (immutable) - string
		Frequency - golang Duration parsed from "1h" "5m"



*/

// A job that needs to be scheduled during the day
type Job struct {
	// A unique identifier for the job that doesn't change often
	ID string

	// How often the job should be run; parsed from strings like "5m" "1h"
	// Should evenly divide the 24 hours in a day
	Frequency time.Duration

	// Calculated start times via the job leveler
	StartTimes []time.Duration
}

// A slice of Jobs
type Jobs []Job

// Returns the duration past midnight when the Job's first start time occurs
func (j *Job) GetFirstStartTime() time.Duration {
	// determine hash of the job ID
	hash := xxhash.Sum64([]byte(j.ID))

	// scale to the job frequency so that the first time begins close to midnight
	dayToFrequencyScaler := float64(j.Frequency) / float64(dayDuration)

	// scale the hash
	hashScaled := float64(hash) * hashToDayDurationScaler * dayToFrequencyScaler

	return time.Duration(hashScaled)
}

// Calculate the number of runs per day for a job
func (j *Job) RunsPerDay() int32 {
	// return int32(math.Floor(float64(dayDuration) / float64(j.Frequency)))
	return int32(dayDuration / j.Frequency)
}

// Set job start times based upon ID hash and frequency
func (j *Job) ScheduleJob() {
	j.StartTimes = make([]time.Duration, j.RunsPerDay())
	j.StartTimes[0] = j.GetFirstStartTime()

	for i := 1; i < int(j.RunsPerDay()); i++ {
		j.StartTimes[i] = j.StartTimes[i-1] + j.Frequency
	}

}

// func (jobs Jobs) GetFirstStartTimes() []time.Duration {
// 	starts := make([]time.Duration, len(jobs))

// 	for i, job := range jobs {
// 		starts[i] = job.GetFirstStartTime()
// 	}

// 	return starts
// }

// Set start times for each job
func (jobs Jobs) ScheduleJobs() {
	for i, _ := range jobs {
		jobs[i].ScheduleJob()
	}
}

// Determine whether a job is started between a range of times
func (j *Job) StartsBetween(fromTime, toTime time.Time) bool {
	// Determine the duration after midnight UTC for the endpoints
	from := fromTime.Sub(fromTime.Truncate(time.Hour * 24))
	to := toTime.Sub(toTime.Truncate(time.Hour * 24))
	// log.Printf("from %v to %v", from, to)
	// 1 to 3h
	// 22h to 3h
	// start time 2h
	// start time 23h

	// for any of the job startimes
	for _, start := range j.StartTimes {
		// if the start time falls between the endpoints
		if from <= start && start < to {
			// log.Printf("start %s matched condition 1", start)
			return true
		}

		// if from and to straddle 0, check that it doesn't fall outside them
		if from > to && !(to <= start && start < from) {
			// log.Printf("start %s matched condition 2", start)
			return true
		}
	}
	return false
}
