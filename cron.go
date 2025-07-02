package cron

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// Cron keeps track of any number of entries, invoking the associated func as
// specified by the schedule. It may be started, stopped, and the entries may
// be inspected while running.
type Cron struct {
	entries          []*Entry
	chain            Chain
	stop             chan struct{}
	add              chan *Entry
	remove           chan EntryID
	snapshot         chan chan []Entry
	running          bool
	runningMu        sync.RWMutex
	location         *time.Location
	parser           ScheduleParser
	nextID           EntryID
	jobWaiter        sync.WaitGroup
	customTime       *time.Time
	manualUpdateNext bool
	lastActivityTime time.Time
	activityMu       sync.RWMutex
}

// ScheduleParser is an interface for schedule spec parsers that return a Schedule
type ScheduleParser interface {
	Parse(spec string) (Schedule, error)
}

// Job is an interface for submitted cron jobs.
type Job interface {
	Run()
}

// Schedule describes a job's duty cycle.
type Schedule interface {
	// Next returns the next activation time, later than the given time.
	// Next is invoked initially, and then each time the job is run.
	Next(time.Time) time.Time
}

// EntryID identifies an entry within a Cron instance
type EntryID int

// Entry consists of a schedule and the func to execute on that schedule.
type Entry struct {
	// ID is the cron-assigned ID of this entry, which may be used to look up a
	// snapshot or remove it.
	ID EntryID

	// Schedule on which this job should be run.
	Schedule Schedule

	// Next time the job will run, or the zero time if Cron has not been
	// started or this entry's schedule is unsatisfiable
	Next time.Time

	// Prev is the last time this job was run, or the zero time if never.
	Prev time.Time

	// WrappedJob is the thing to run when the Schedule is activated.
	WrappedJob Job

	// Job is the thing that was submitted to cron.
	// It is kept around so that user code that needs to get at the job later,
	// e.g. via Entries() can do so.
	Job Job

	// if the entry is set to a specific time and the entry has triggered and does not require to be triggered anymore, then
	// this is set to true
	Completed bool

	// Is waiting for a manual update on the next time
	WaitForManualUpdateNext bool
}

// Valid returns true if this is not the zero entry.
func (e Entry) Valid() bool { return e.ID != 0 }

// byTime is a wrapper for sorting the entry array by time
// (with zero time at the end).
type byTime []*Entry

func (s byTime) Len() int      { return len(s) }
func (s byTime) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byTime) Less(i, j int) bool {
	// Two zero times should return false.
	// Otherwise, zero is "greater" than any other time.
	// (To sort it at the end of the list.)
	if s[i].Next.IsZero() || s[i].Completed {
		return false
	}
	if s[j].Next.IsZero() || s[j].Completed {
		return true
	}
	return s[i].Next.Before(s[j].Next)
}

// New returns a new Cron job runner, modified by the given options.
//
// Available Settings
//
//	Time Zone
//	  Description: The time zone in which schedules are interpreted
//	  Default:     time.Local
//
//	Parser
//	  Description: Parser converts cron spec strings into cron.Schedules.
//	  Default:     Accepts this spec: https://en.wikipedia.org/wiki/Cron
//
//	Chain
//	  Description: Wrap submitted jobs to customize behavior.
//	  Default:     A chain that recovers panics and logs them to stderr.
//
// See "cron.With*" to modify the default behavior.
func New(opts ...Option) *Cron {
	c := &Cron{
		entries:          nil,
		chain:            NewChain(),
		add:              make(chan *Entry),
		stop:             make(chan struct{}),
		snapshot:         make(chan chan []Entry),
		remove:           make(chan EntryID),
		running:          false,
		runningMu:        sync.RWMutex{},
		location:         time.Local,
		parser:           standardParser,
		customTime:       nil,
		manualUpdateNext: false,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// FuncJob is a wrapper that turns a func() into a cron.Job
type FuncJob func()

func (f FuncJob) Run() { f() }

// AddFunc adds a func to the Cron to be run on the given schedule.
// The spec is parsed using the time zone of this Cron instance as the default.
// An opaque ID is returned that can be used to later remove it.
func (c *Cron) AddFunc(spec string, cmd func()) (EntryID, error) {
	return c.AddJob(spec, FuncJob(cmd))
}

// AddJob adds a Job to the Cron to be run on the given schedule.
// The spec is parsed using the time zone of this Cron instance as the default.
// An opaque ID is returned that can be used to later remove it.
func (c *Cron) AddJob(spec string, cmd Job) (EntryID, error) {
	schedule, err := c.parser.Parse(spec)
	if err != nil {
		return 0, err
	}
	return c.Schedule(schedule, cmd), nil
}

// Schedule adds a Job to the Cron to be run on the given schedule.
// The job is wrapped with the configured Chain.
func (c *Cron) Schedule(schedule Schedule, cmd Job) EntryID {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	c.nextID++
	entry := &Entry{
		ID:         c.nextID,
		Schedule:   schedule,
		WrappedJob: c.chain.Then(cmd),
		Job:        cmd,
	}
	entry.Next = entry.Schedule.Next(c.now())
	if !c.running {
		c.entries = append(c.entries, entry)
	} else {
		c.add <- entry
	}
	return entry.ID
}

// Entries returns a snapshot of the cron entries.
func (c *Cron) Entries() []Entry {
	c.runningMu.RLock()
	defer c.runningMu.RUnlock()
	if c.running {
		replyChan := make(chan []Entry, 1)
		c.snapshot <- replyChan
		return <-replyChan
	}
	return c.entrySnapshot()
}

// Location gets the time zone location
func (c *Cron) Location() *time.Location {
	return c.location
}

// Entry returns a snapshot of the given entry, or nil if it couldn't be found.
func (c *Cron) Entry(id EntryID) Entry {
	for _, entry := range c.Entries() {
		if id == entry.ID {
			return entry
		}
	}
	return Entry{}
}

// Remove an entry from being run in the future.
func (c *Cron) Remove(id EntryID) {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		c.remove <- id
	} else {
		c.removeEntry(id)
	}
}

// Start the cron scheduler in its own goroutine, or no-op if already started.
func (c *Cron) Start() {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		return
	}
	c.running = true
	go c.run()
}

// Run the cron scheduler, or no-op if already running.
func (c *Cron) Run() {
	c.runningMu.Lock()
	if c.running {
		c.runningMu.Unlock()
		return
	}
	c.running = true
	c.runningMu.Unlock()
	c.run()
}

// run the scheduler.. this is private just due to the need to synchronize
// access to the 'running' state variable.
func (c *Cron) run() {
	log.Info().Msg("cron - start")

	// Start health check ticker that triggers every 10 seconds
	heartbeatChannel := c.startHeartbeat()

	// Update last activity time whenever the timer fires
	updateActivity := func() {
		c.activityMu.Lock()
		defer c.activityMu.Unlock()
		c.lastActivityTime = time.Now()
	}

	// Figure out the next activation times for each entry.
	now := c.now()
	for _, entry := range c.entries {
		entry.Next = entry.Schedule.Next(now)
		log.Info().Time("now", now).Int64("entry", int64(entry.ID)).Time("next", entry.Next).Msg("cron - schedule")
	}

	for {
		// Determine the next entry to run.
		sort.Sort(byTime(c.entries))

		var timer *time.Timer
		if len(c.entries) == 0 || c.entries[0].Next.IsZero() {
			// If there are no entries yet, just sleep - it still handles new entries
			// and stop requests.
			timer = time.NewTimer(100000 * time.Hour)
		} else {
			for _, entry := range c.entries {
				if entry.WaitForManualUpdateNext {
					continue
				}
				now := c.now()
				timer = time.NewTimer(entry.Next.Sub(now))
				break
			}
			if timer == nil {
				time.Sleep(200 * time.Millisecond)
				continue
			}
		}

		for {
			select {
			case now = <-timer.C:
				updateActivity()
				now = now.In(c.location)
				log.Info().Time("now", now).Msg("cron - wake")

				// Run every entry whose next time was less than now
				for _, e := range c.entries {
					if e.Next.After(now) || e.Next.IsZero() {
						break
					}
					c.startJob(e.WrappedJob)
					if !c.manualUpdateNext {
						e.Prev = e.Next
						e.Next = e.Schedule.Next(now)
						if e.Next.Before(now) {
							e.Completed = true
						}
					} else {
						e.WaitForManualUpdateNext = true
					}
					log.Info().Time("now", now).Int64("entry", int64(e.ID)).Time("next", e.Next).Msg("cron - run")
				}

			case newEntry := <-c.add:
				timer.Stop()
				now = c.now()
				newEntry.Next = newEntry.Schedule.Next(now)
				c.entries = append(c.entries, newEntry)
				log.Info().Time("now", now).Int64("entry", int64(newEntry.ID)).Time("next", newEntry.Next).Msg("cron - added")

			case replyChan := <-c.snapshot:
				replyChan <- c.entrySnapshot()
				continue

			case <-c.stop:
				timer.Stop()
				log.Info().Msg("cron - stop")
				return

			case id := <-c.remove:
				timer.Stop()
				now = c.now()
				c.removeEntry(id)
				log.Info().Int64("entry", int64(id)).Msg("cron - removed")

			case <-heartbeatChannel:
				// we need to trigger this so a new timer is created
				timer.Stop()
			}

			break
		}
	}
}

// startHeartbeat is a ticker that fires at 2, 12, 22, 32, 42, 52 seconds
// the offset is so that the normal timer has time to fire first.
func (c *Cron) startHeartbeat() chan bool {
	heartbeatChannel := make(chan bool)
	go func() {
		for {
			if !c.running {
				break
			}
			now := c.now()
			// Calculate duration until next target second
			sec := now.Second()
			var nextTick time.Duration

			// Find the next target second in this minute
			switch {
			case sec < 5:
				nextTick = 5 - time.Duration(sec)
			case sec < 15:
				nextTick = 15 - time.Duration(sec)
			case sec < 25:
				nextTick = 25 - time.Duration(sec)
			case sec < 35:
				nextTick = 35 - time.Duration(sec)
			case sec < 45:
				nextTick = 45 - time.Duration(sec)
			case sec < 55:
				nextTick = 55 - time.Duration(sec)
			default: // 52-59 seconds
				nextTick = 65 - time.Duration(sec) // Next minute's first tick
			}

			// Wait until the next target second
			time.Sleep(time.Duration(nextTick) * time.Second)

			// Execute your task here
			c.activityMu.RLock()
			now = c.now()
			timeSinceLastActivity := now.Sub(c.lastActivityTime)
			c.activityMu.RUnlock()

			if !c.running {
				break
			}

			if timeSinceLastActivity > 10*time.Second {
				// check if there are any entries that should have fired but didn't
				for _, e := range c.entries {
					if e.Next.Before(now) && !e.Completed {
						now = c.now().In(c.location)
						log.Error().Interface("entry", e).Time("now", now).Time("next", e.Next).Msg("cron - entry should have fired but didn't")
						c.startJob(e.WrappedJob)
						if !c.manualUpdateNext {
							e.Prev = e.Next
							e.Next = e.Schedule.Next(now)
							if e.Next.Before(now) {
								e.Completed = true
							}
						} else {
							e.WaitForManualUpdateNext = true
						}
						break
					}
				}
			}

			// Add a small delay to prevent tight loop if the task completes quickly
			time.Sleep(100 * time.Millisecond)
			heartbeatChannel <- true
		}
	}()
	return heartbeatChannel
}

// startJob runs the given job in a new goroutine.
func (c *Cron) startJob(j Job) {
	c.jobWaiter.Add(1)
	go func() {
		defer c.jobWaiter.Done()
		j.Run()
	}()
}

// now returns current time in c location
func (c *Cron) now() time.Time {
	if c.customTime != nil {
		tmp := *c.customTime
		return tmp.In(c.location)
	}
	return time.Now().In(c.location)
}

// Stop stops the cron scheduler if it is running; otherwise it does nothing.
// A context is returned so the caller can wait for running jobs to complete.
func (c *Cron) Stop() context.Context {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		c.stop <- struct{}{}
		c.running = false
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		c.jobWaiter.Wait()
		cancel()
	}()
	return ctx
}

// entrySnapshot returns a copy of the current cron entry list.
func (c *Cron) entrySnapshot() []Entry {
	var entries = make([]Entry, len(c.entries))
	for i, e := range c.entries {
		entries[i] = *e
	}
	return entries
}

func (c *Cron) removeEntry(id EntryID) {
	var entries []*Entry
	for _, e := range c.entries {
		if e.ID != id {
			entries = append(entries, e)
		}
	}
	c.entries = entries
}

func (c *Cron) EntriesToFire() []*Entry {
	log.Info().Msg("cron - entriesToFire - start")
	c.runningMu.RLock()
	defer c.runningMu.RUnlock()
	var entriesToFire []*Entry
	for _, e := range c.entries {
		now := c.now()
		if e.Next.After(now) || e.Next.IsZero() {
			continue
		}
		entriesToFire = append(entriesToFire, e)
	}
	log.Info().Interface("entriesToFire", entriesToFire).Msg("cron - entriesToFire - end")
	return entriesToFire
}

func (c *Cron) UpdateNextSchedule(entry *Entry) {
	log.Info().Msg("cron - update next schedule")
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	entry.Prev = entry.Next
	entry.Next = entry.Schedule.Next(entry.Prev)
	if entry.Next.Before(c.now()) {
		entry.Completed = true
	}
	entry.WaitForManualUpdateNext = false
}

func (c *Cron) GetCustomTime() *time.Time {
	return c.customTime
}

func (c *Cron) SetCustomTime(time time.Time) {
	c.customTime = &time
}

func (c *Cron) UpdateAllNextSchedules() {
	log.Info().Msg("cron - update all next schedules")
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	for _, e := range c.entries {
		e.Next = e.Schedule.Next(c.now())
		if e.Next.Before(c.now()) {
			e.Completed = true
		}
		e.WaitForManualUpdateNext = false
	}
}
