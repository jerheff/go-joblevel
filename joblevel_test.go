package joblevel

import (
	"math/rand"
	"strconv"
	"testing"
	"time"

	. "github.com/franela/goblin"
)

func Test(t *testing.T) {
	g := Goblin(t)

	g.Describe("Job", func() {
		g.It("should determine how many times a day a job runs", func() {
			job := Job{Frequency: time.Hour}
			g.Assert(job.RunsPerDay()).Equal(24)

			job = Job{Frequency: time.Minute * 5}
			g.Assert(job.RunsPerDay()).Equal(24 * 12)
		})

		g.It("should schedule the first start time near midnight", func() {
			job := Job{Frequency: time.Second}
			job.ScheduleJob()
			g.Assert(job.StartTimes[0] < time.Second).IsTrue()
		})

		g.It("should schedule start times at the correct frequency", func() {
			job := Job{Frequency: time.Minute * 6}
			job.ScheduleJob()
			g.Assert(job.StartTimes[1]-job.StartTimes[0] == time.Minute*6).IsTrue()
		})

		g.It("should schedule the right number of start times", func() {
			job := Job{Frequency: time.Hour * 2}
			job.ScheduleJob()
			g.Assert(len(job.StartTimes)).Equal(12)
		})
	})

	job := Job{Frequency: time.Hour, StartTimes: []time.Duration{time.Hour}}

	job = Job{Frequency: time.Hour, StartTimes: []time.Duration{time.Hour}}
	// job.ScheduleJob()
	now := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

	g.Describe("Job", func() {
		g.It("should recognize a job starting between two times", func() {
			g.Assert(job.StartsBetween(now.Add(time.Minute), now.Add(time.Hour*2))).IsTrue()
		})

		g.It("should recognize a job starting between two times crossing midnight", func() {
			g.Assert(job.StartsBetween(now.Add(time.Hour*23), now.Add(time.Hour*26))).IsTrue()
		})

		g.It("should recognize a job not starting between two times", func() {
			g.Assert(job.StartsBetween(now.Add(time.Hour+time.Minute), now.Add(time.Hour*2))).IsFalse()
		})

		g.It("should recognize a job not starting between two times crossing midnight", func() {
			g.Assert(job.StartsBetween(now.Add(time.Hour*23), now.Add(time.Hour*24+time.Minute*59))).IsFalse()
		})
	})

	g.Describe("Jobs", func() {
		jobs := Jobs{Job{ID: "1", Frequency: time.Hour}, Job{ID: "2", Frequency: time.Hour}}
		jobs.ScheduleJobs()

		g.It("should be scheduled at different times even for the same frequency", func() {
			g.Assert(jobs[0].StartTimes[0] == jobs[1].StartTimes[0]).IsFalse()
		})

		g.It("should determine which jobs to run", func() {
			toRun := jobs.StartingBetween(now, now.Add(time.Minute*30)).IDs()
			g.Assert(len(toRun) != len(jobs)).IsTrue()
		})

		g.It("should work across many job frequencies", func() {
			const n = 1000
			randomJobs := make(Jobs, n)
			for i := 0; i < n; i++ {
				randomJobs[i] = Job{ID: strconv.Itoa(i), Frequency: time.Minute * time.Duration(1+rand.Intn(24*60))}
			}
			randomJobs.ScheduleJobs()
			for i := 1; i < n; i++ {
				g.Assert(randomJobs[i-1].StartTimes[0] != randomJobs[i].StartTimes[0]).IsTrue()
			}
		})
	})
}
