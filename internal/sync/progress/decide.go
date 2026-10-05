// Package progress pushes reader progress to trackers.
package progress

import (
	"strconv"
	"strings"

	"mangasync/internal/core"
)

// Target is the tracker state the reader's progress implies.
type Target struct {
	Status   core.Status
	Progress *float64 // nil = don't send progress
	Unit     core.Unit
}

// ComputeTarget maps reader progress to a target status/progress. ended is the tracker's
// publication status and only matters when every book is read. Returns nil if nothing was read.
func ComputeTarget(p core.ReadProgress, ended bool) *Target {
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
		return nil
	}
	return t
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
func Decide(cur *core.Entry, t Target) *core.EntryUpdate {
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
	return strings.Join(parts, " ")
}

func describeFloat(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }
