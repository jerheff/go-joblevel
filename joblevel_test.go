package joblevel

import (
	"testing"
	"time"

	. "github.com/franela/goblin"
)

func Test(t *testing.T) {
	g := Goblin(t)

	g.Describe("Job", func() {
		g.It("should determine how many times a day a job runs", func() {
			g.Assert(Job{Frequency: time.Hour}.RunsPerDay()).Equal(24)
		})
	})

	job := Job{Frequency: time.Hour, StartTimes: []time.Duration{time.Hour}}
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
}
