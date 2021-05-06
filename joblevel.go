package joblevel

import (
	"encoding/csv"
	"errors"
	"io"
	"math"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/gocarina/gocsv"
)

// time.Duration of a day
const dayDuration = 24 * time.Hour

// multiplier to scale the max Int64 (largest hash possible) to the max duration of a day
const hashToDayDurationScaler = float64(dayDuration) / math.MaxUint64

// a series of start times across the day
type startDurations []time.Duration

// A job that needs to be scheduled during the day
type Job struct {
	// A unique identifier for the job that doesn't change often
	ID string

	// How often the job should be run; parsed from strings like "5m" "1h"
	// Should evenly divide the 24 hours in a day
	Frequency time.Duration

	// Calculated start times across the day
	starts startDurations
}

// A slice of Jobs
type Jobs []Job

// Returns the duration past midnight when the Job's first start time occurs
func (j *Job) getFirstStart() time.Duration {
	// determine hash of the job ID
	hash := xxhash.Sum64([]byte(j.ID))

	// scale to the job frequency so that the first time begins close to midnight
	dayToFrequencyScaler := float64(j.Frequency) / float64(dayDuration)

	// scale the hash
	hashScaled := float64(hash) * hashToDayDurationScaler * dayToFrequencyScaler

	return time.Duration(hashScaled)
}

// Calculate the number of runs per day for a job
func (j *Job) RunsPerDay() int {
	// return int32(math.Floor(float64(dayDuration) / float64(j.Frequency)))
	return int(dayDuration / j.Frequency)
}

// Set job start times based upon ID hash and frequency
func (j *Job) ScheduleJob() {
	j.starts = make(startDurations, j.RunsPerDay())
	j.starts[0] = j.getFirstStart()

	for i := 1; i < j.RunsPerDay(); i++ {
		j.starts[i] = j.starts[i-1] + j.Frequency
	}

}

// Set start times for each job
func (jobs Jobs) ScheduleJobs() {
	for i, _ := range jobs {
		jobs[i].ScheduleJob()
	}
}

// Determine whether a job is started between a range of times
func (j *Job) StartsBetween(fromTime, toTime time.Time) (bool, error) {
	if !fromTime.Before(toTime) {
		return false, errors.New("fromTime must preceed toTime")
	}
	// Determine the duration after midnight UTC for the endpoints
	from := fromTime.In(time.UTC).Sub(fromTime.Truncate(time.Hour * 24))
	to := toTime.In(time.UTC).Sub(toTime.Truncate(time.Hour * 24))
	// log.Printf("from %v to %v", from, to)

	// for any of the job startimes
	filteredStarts := j.starts.startsBetween(from, to, true)
	return len(filteredStarts) > 0, nil
}

// Filters to start times that occur between the given range
func (starts startDurations) startsBetween(from, to time.Duration, firstOnly bool) startDurations {
	filteredStarts := make([]time.Duration, 0)

	for _, s := range starts {
		// if the start time falls between the endpoints
		if from <= s && s < to {
			// log.Printf("start %s matched condition 1", start)
			filteredStarts = append(filteredStarts, s)
		}

		// if from and to straddle 0, check that the start time doesn't fall outside them
		if from > to && !(to <= s && s < from) {
			// log.Printf("start %s matched condition 2", start)
			filteredStarts = append(filteredStarts, s)
		}

		// early return if we only care about one start
		if firstOnly && len(filteredStarts) > 0 {
			return filteredStarts
		}
	}

	return filteredStarts
}

// Filter jobs to those that start between the given times
func (jobs Jobs) StartingBetween(fromTime, toTime time.Time) Jobs {
	startingJobs := make(Jobs, 0)
	for _, j := range jobs {
		b, _ := j.StartsBetween(fromTime, toTime)
		if b {
			startingJobs = append(startingJobs, j)
		}
	}
	return startingJobs
}

// Find durationg containing the given time
func DurationContaining(d time.Duration, t time.Time) (fromTime, toTime time.Time) {
	fromTime = t.Truncate(d)
	toTime = fromTime.Add(d)
	return
}

// Filters jobs to those starting during the rounded duration containing the given time
// For instance, setting 12:07pm and a 1 hour duration returns jobs between noon and 1pm
func (jobs Jobs) StartingDuringDuration(t time.Time, d time.Duration) Jobs {
	fromTime, toTime := DurationContaining(d, t)
	return jobs.StartingBetween(fromTime, toTime)
}

// Return the IDs for the provided jobs
func (jobs Jobs) IDs() []string {
	IDs := make([]string, 0)
	for i, _ := range jobs {
		IDs = append(IDs, jobs[i].ID)
	}
	return IDs
}

// Deduplicate a slice of strings
func Deduplicate(values []string) []string {
	keys := make(map[string]bool)
	outputs := []string{}

	for _, s := range values {
		if _, value := keys[s]; !value {
			keys[s] = true
			outputs = append(outputs, s)
		}
	}

	return outputs
}

// Return all start durations for a given set of Jobs
func (jobs Jobs) AllStarts() startDurations {
	starts := make(startDurations, 0)
	for i, _ := range jobs {
		starts = append(starts, jobs[i].starts...)
	}
	return starts
}

// Returns a record for each start time of each Job
func (jobs Jobs) ScheduledStartRecords() [][]string {
	records := make([][]string, 0)
	for _, j := range jobs {
		for _, s := range j.starts {
			// For fraction of day values: strconv.FormatFloat(float64(s)/float64(dayDuration), 'f', -1, 64)
			records = append(records, []string{j.ID, s.String()})
		}
	}
	return records
}

// Create new Job with given ID
func NewJob(id string) *Job {
	return &Job{ID: id}
}

func (j Job) WithFrequency(frequency interface{}) Job {
	switch v := frequency.(type) {
	case string:
		d, err := time.ParseDuration(v)
		if err != nil {
			panic(err)
		}
		j.Frequency = d
	case time.Duration:
		j.Frequency = v
	default:
		panic(errors.New("Unknown frequency type"))
	}
	return j
}

// Load Jobs from a CSV
func NewJobsFromCSV(r io.Reader) Jobs {
	newJobs := make(Jobs, 0)

	type csvJobs struct {
		ID        string
		Frequency string
	}

	jobs := make([]csvJobs, 0)

	if err := gocsv.UnmarshalCSV(gocsv.DefaultCSVReader(r), &jobs); err != nil {
		panic(err)
	}

	for _, j := range jobs {
		newJobs = append(newJobs, NewJob(j.ID).WithFrequency(j.Frequency))
	}
	return newJobs
}

// Write a CSV of Jobs to the provided io.Writer
func (jobs Jobs) CSV(w io.Writer) error {
	s, err := gocsv.MarshalString(jobs)
	if err != nil {
		return err
	}
	_, err = io.WriteString(w, s)
	if err != nil {
		return err
	}
	return nil
}

// Write CSV of Job start IDs and starts
// with one line per job start duration
func (jobs Jobs) ScheduleCSV(w io.Writer) {
	csvwriter := csv.NewWriter(w)
	csvwriter.Write([]string{"ID", "StartDurationAfterMidnightUTC"})
	csvwriter.WriteAll(jobs.ScheduledStartRecords())
	csvwriter.Flush()
}
