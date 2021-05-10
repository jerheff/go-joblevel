package joblevel

import (
	"bytes"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/franela/goblin"
)

func Test(t *testing.T) {
	g := Goblin(t)

	now := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

	g.Describe("Job", func() {
		g.It("should be created correctly from a duration string", func() {
			job := NewJob("abc").WithFrequency("5m")
			g.Assert(job.ID).Equal("abc")
			g.Assert(job.Frequency).Equal(time.Minute * 5)
		})

		g.It("should be created correctly from a time.Duration", func() {
			job := NewJob("abc").WithFrequency(time.Hour)
			g.Assert(job.ID).Equal("abc")
			g.Assert(job.Frequency).Equal(time.Hour)
		})

		g.It("should determine how many times a day a job runs", func() {
			job := Job{Frequency: time.Hour}
			g.Assert(job.RunsPerDay()).Equal(24)

			job = Job{Frequency: time.Minute * 5}
			g.Assert(job.RunsPerDay()).Equal(24 * 12)
		})

		g.It("should schedule the first start time near midnight", func() {
			job := Job{Frequency: time.Second}
			job.ScheduleJob()
			g.Assert(job.starts[0] < time.Second).IsTrue()
		})

		g.It("should schedule start times at the correct frequency", func() {
			job := Job{Frequency: time.Minute * 6}
			job.ScheduleJob()
			g.Assert(job.starts[1]-job.starts[0] == time.Minute*6).IsTrue()
		})

		g.It("should schedule the right number of start times", func() {
			job := Job{Frequency: time.Hour * 2}
			job.ScheduleJob()
			g.Assert(len(job.starts)).Equal(12)
		})
	})

	g.Describe("Job:StartsBetween", func() {
		job := Job{Frequency: time.Hour, starts: StartDurations{time.Hour}}

		g.It("should error when from is after to", func() {
			_, err := job.StartsBetween(now.Add(time.Hour), now.Add(time.Minute))
			g.Assert(err).IsNotNil()
		})

		g.It("should error when from equals to", func() {
			_, err := job.StartsBetween(now.Add(time.Hour), now.Add(time.Hour))
			g.Assert(err).IsNotNil()
		})

		g.It("should recognize a job starting between two times", func() {
			b, err := job.StartsBetween(now.Add(time.Minute), now.Add(time.Hour*2))
			g.Assert(b).IsTrue()
			g.Assert(err).IsNil()
		})

		g.It("should recognize a job starting between two times crossing midnight", func() {
			b, err := job.StartsBetween(now.Add(time.Hour*23), now.Add(time.Hour*26))
			g.Assert(b).IsTrue()
			g.Assert(err).IsNil()
		})

		g.It("should recognize a job not starting between two times", func() {
			b, err := job.StartsBetween(now.Add(time.Hour+time.Minute), now.Add(time.Hour*2))
			g.Assert(b).IsFalse()
			g.Assert(err).IsNil()
		})

		g.It("should recognize a job not starting between two times crossing midnight", func() {
			b, err := job.StartsBetween(now.Add(time.Hour*23), now.Add(time.Hour*24+time.Minute*59))
			g.Assert(b).IsFalse()
			g.Assert(err).IsNil()
		})
	})

	g.Describe("Jobs", func() {
		jobs := Jobs{Job{ID: "1", Frequency: time.Hour}, Job{ID: "2", Frequency: time.Hour}}
		jobs.ScheduleJobs()

		g.It("should be loaded from and saved to CSV properly", func() {
			csv := strings.NewReader("ID,Frequency\na,5m\nb,1h")
			jobs := NewJobsFromCSV(csv)
			g.Assert(jobs[0].ID).Equal("a")
			g.Assert(jobs[0].Frequency).Equal(time.Minute * 5)
			g.Assert(jobs[1].ID).Equal("b")
			g.Assert(jobs[1].Frequency).Equal(time.Hour)

			var buf bytes.Buffer
			err := jobs.CSV(&buf)
			g.Assert(err).IsNil()

			roundtripJobs := NewJobsFromCSV(strings.NewReader(buf.String()))
			g.Assert(roundtripJobs).Equal(jobs)
		})

		g.It("should save a schedule CSV properly", func() {
			var buf bytes.Buffer
			err := jobs.ScheduleCSV(&buf)
			g.Assert(err).IsNil()
		})

		g.It("should be scheduled at different times even for the same frequency", func() {
			g.Assert(jobs[0].starts[0] == jobs[1].starts[0]).IsFalse()
		})

		g.It("should determine which jobs to run between two times", func() {
			toRun := jobs.StartingBetween(now, now.Add(time.Minute*30)).IDs()
			g.Assert(len(toRun) != len(jobs)).IsTrue()
		})

		g.It("should determine which jobs to run during a rounded duration", func() {
			toRun := jobs.StartingDuringDuration(now, time.Hour).IDs()
			g.Assert(len(toRun)).Equal(len(jobs))
		})

		g.It("should work across many job frequencies", func() {
			const n = 1000
			randomJobs := make(Jobs, n)
			for i := 0; i < n; i++ {
				randomJobs[i] = Job{ID: strconv.Itoa(i), Frequency: time.Minute * time.Duration(1+rand.Intn(24*60))}
			}
			randomJobs.ScheduleJobs()
			for i := 1; i < n; i++ {
				g.Assert(randomJobs[i-1].starts[0] != randomJobs[i].starts[0]).IsTrue()
			}
		})

		g.It("should deduplicate IDs properly", func() {
			IDs := append(jobs, jobs...).IDs()
			deduplicatedIDs := Deduplicate(IDs)

			g.Assert(len(IDs) == 2*len(deduplicatedIDs)).IsTrue()
		})
	})
}
