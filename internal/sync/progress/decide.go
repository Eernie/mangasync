// Package progress pushes reader progress to trackers.
package progress

import (
	"strconv"
	"strings"
	"time"

	"mangasync/internal/core"
)

// Target is the tracker state the reader's progress implies.
type Target struct {
	Status   core.Status
	Progress *float64 // nil = don't send progress
	Unit     core.Unit
	// StartDate and FinishDate are civil dates (YYYY-MM-DD) or "". FinishDate is only set
	// when Status is completed.
	StartDate  string
	FinishDate string
}

const dateLayout = "2006-01-02"

// ComputeTarget maps reader progress to a target status/progress/dates. ended is the tracker's
// publication status and only matters when every book is read. Dates are calendar dates in loc
// (nil = time.Local); the finish date is only set for a completed target.
// Nothing read and nothing in progress gives a planning target without progress or dates;
// a series without any books gives nil.
func ComputeTarget(p core.ReadProgress, ended bool, loc *time.Location) *Target {
	if p.BooksTotal == 0 {
		return nil
	}
	t := &Target{Unit: p.Unit}
	switch {
	case p.AllRead():
		t.Status = core.StatusReading
		if ended {
			t.Status = core.StatusCompleted
		}
		t.Progress = positive(p.MaxNumber)
	case p.BooksRead > 0:
		t.Status = core.StatusReading
		t.Progress = positive(p.LastReadNumber)
	case p.BooksInProgress > 0:
		t.Status = core.StatusReading
	default:
		return &Target{Status: core.StatusPlanning, Unit: p.Unit}
	}
	if loc == nil {
		loc = time.Local
	}
	t.StartDate = civilDate(p.FirstReadAt, loc)
	if t.Status == core.StatusCompleted {
		t.FinishDate = civilDate(p.LastReadAt, loc)
	}
	return t
}

// civilDate formats the calendar date of ts in loc, or "" for the zero time.
func civilDate(ts time.Time, loc *time.Location) string {
	if ts.IsZero() {
		return ""
	}
	return ts.In(loc).Format(dateLayout)
}

// positive returns nil for values <= 0: trackers store 0 as "nothing recorded".
func positive(v float64) *float64 {
	if v <= 0 {
		return nil
	}
	return &v
}

// Decide returns the update that moves cur towards t, or nil if nothing should change.
// Protected statuses are never touched, status never moves backwards and progress never goes down.
// Dates are only filled in when the entry has none yet; an existing date is never overwritten.
// A planning target only creates a missing entry: any existing entry, whatever its status, is left alone.
func Decide(cur *core.Entry, t Target) *core.EntryUpdate {
	if t.Status == core.StatusPlanning {
		if cur != nil {
			return nil
		}
		s := core.StatusPlanning
		return &core.EntryUpdate{Status: &s}
	}
	if cur != nil {
		switch cur.Status {
		case core.StatusConsidering, core.StatusPlanning, core.StatusReading:
		default:
			return nil
		}
	}
	var u core.EntryUpdate
	changed := false

	var curStatus core.Status
	if cur != nil {
		curStatus = cur.Status
	}
	if rank(t.Status) > rank(curStatus) {
		s := t.Status
		u.Status = &s
		changed = true
	}

	if t.Progress != nil {
		var curProgress *float64
		if cur != nil {
			curProgress = cur.Chapter
			if t.Unit == core.UnitVolume {
				curProgress = cur.Volume
			}
		}
		if curProgress == nil || *t.Progress > *curProgress {
			p := *t.Progress
			if t.Unit == core.UnitVolume {
				u.Volume = &p
			} else {
				u.Chapter = &p
			}
			changed = true
		}
	}
	if t.StartDate != "" && (cur == nil || cur.StartDate == "") {
		d := t.StartDate
		u.StartDate = &d
		changed = true
	}
	// Never send a finish date earlier than a start date the user set (YYYY-MM-DD compares as text).
	if t.FinishDate != "" && t.Status == core.StatusCompleted && (cur == nil || cur.FinishDate == "") &&
		(cur == nil || cur.StartDate == "" || t.FinishDate >= cur.StartDate) {
		d := t.FinishDate
		u.FinishDate = &d
		changed = true
	}
	if !changed {
		return nil
	}
	return &u
}

func rank(s core.Status) int {
	switch s {
	case core.StatusReading:
		return 1
	case core.StatusCompleted:
		return 2
	}
	return 0
}

// describe renders an update for logs, e.g. "status=reading chapter=104".
func describe(u core.EntryUpdate) string {
	var parts []string
	if u.Status != nil {
		parts = append(parts, "status="+string(*u.Status))
	}
	if u.Chapter != nil {
		parts = append(parts, "chapter="+describeFloat(*u.Chapter))
	}
	if u.Volume != nil {
		parts = append(parts, "volume="+describeFloat(*u.Volume))
	}
	if u.StartDate != nil {
		parts = append(parts, "start="+*u.StartDate)
	}
	if u.FinishDate != nil {
		parts = append(parts, "finish="+*u.FinishDate)
	}
	return strings.Join(parts, " ")
}

func describeFloat(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }
