package progress

import (
	"testing"
	"time"

	"mangasync/internal/core"
)

func f(v float64) *float64 { return &v }

func TestComputeTarget(t *testing.T) {
	ch := core.UnitChapter
	cases := []struct {
		name  string
		p     core.ReadProgress
		ended bool
		want  *Target
	}{
		{"nothing read", core.ReadProgress{Unit: ch, BooksTotal: 10}, false, nil},
		{"first book partly read", core.ReadProgress{Unit: ch, BooksTotal: 10, BooksInProgress: 1}, false,
			&Target{Status: core.StatusReading, Unit: ch}},
		{"some read", core.ReadProgress{Unit: ch, BooksTotal: 244, BooksRead: 104, LastReadNumber: 104, MaxNumber: 232}, false,
			&Target{Status: core.StatusReading, Progress: f(104), Unit: ch}},
		{"all read, ongoing", core.ReadProgress{Unit: ch, BooksTotal: 85, BooksRead: 85, LastReadNumber: 85, MaxNumber: 85}, false,
			&Target{Status: core.StatusReading, Progress: f(85), Unit: ch}},
		{"all read, ended", core.ReadProgress{Unit: ch, BooksTotal: 81, BooksRead: 81, LastReadNumber: 80, MaxNumber: 80}, true,
			&Target{Status: core.StatusCompleted, Progress: f(80), Unit: ch}},
		{"read but no continuous number", core.ReadProgress{Unit: ch, BooksTotal: 10, BooksRead: 1, LastReadNumber: 0, MaxNumber: 10}, false,
			&Target{Status: core.StatusReading, Unit: ch}},
	}
	for _, c := range cases {
		got := ComputeTarget(c.p, c.ended, time.UTC)
		if !targetEqual(got, c.want) {
			t.Errorf("%s: got %s, want %s", c.name, fmtTarget(got), fmtTarget(c.want))
		}
	}
}

func TestDecide(t *testing.T) {
	reading := func(p float64) Target {
		return Target{Status: core.StatusReading, Progress: f(p), Unit: core.UnitChapter}
	}
	completed := func(p float64) Target {
		return Target{Status: core.StatusCompleted, Progress: f(p), Unit: core.UnitChapter}
	}
	entry := func(s core.Status, ch *float64) *core.Entry { return &core.Entry{Status: s, Chapter: ch} }

	cases := []struct {
		name       string
		cur        *core.Entry
		target     Target
		wantNil    bool
		wantStatus core.Status // "" = no status change
		wantCh     *float64
		wantVol    *float64
	}{
		{"new entry", nil, reading(5), false, core.StatusReading, f(5), nil},
		{"new entry, no progress", nil, Target{Status: core.StatusReading, Unit: core.UnitChapter}, false, core.StatusReading, nil, nil},
		{"planning -> reading", entry(core.StatusPlanning, nil), reading(5), false, core.StatusReading, f(5), nil},
		{"considering -> completed", entry(core.StatusConsidering, nil), completed(80), false, core.StatusCompleted, f(80), nil},
		{"reading, progress up", entry(core.StatusReading, f(3)), reading(5), false, "", f(5), nil},
		{"reading, progress lower", entry(core.StatusReading, f(10)), reading(5), true, "", nil, nil},
		{"reading, progress equal", entry(core.StatusReading, f(5)), reading(5), true, "", nil, nil},
		{"reading -> completed", entry(core.StatusReading, f(79)), completed(80), false, core.StatusCompleted, f(80), nil},
		{"completed protected", entry(core.StatusCompleted, f(10)), reading(50), true, "", nil, nil},
		{"paused protected", entry(core.StatusPaused, nil), reading(5), true, "", nil, nil},
		{"dropped protected", entry(core.StatusDropped, nil), reading(5), true, "", nil, nil},
		{"rereading protected", entry(core.StatusRereading, nil), reading(5), true, "", nil, nil},
		{"unknown protected", entry(core.StatusUnknown, nil), reading(5), true, "", nil, nil},
		{"volume unit", &core.Entry{Status: core.StatusReading, Chapter: f(100), Volume: f(2)},
			Target{Status: core.StatusReading, Progress: f(3), Unit: core.UnitVolume}, false, "", nil, f(3)},
	}
	for _, c := range cases {
		got := Decide(c.cur, c.target)
		if c.wantNil {
			if got != nil {
				t.Errorf("%s: want nil, got %+v", c.name, *got)
			}
			continue
		}
		if got == nil {
			t.Errorf("%s: got nil", c.name)
			continue
		}
		if (c.wantStatus == "") != (got.Status == nil) || (got.Status != nil && *got.Status != c.wantStatus) {
			t.Errorf("%s: status = %v, want %q", c.name, got.Status, c.wantStatus)
		}
		if !floatPtrEqual(got.Chapter, c.wantCh) || !floatPtrEqual(got.Volume, c.wantVol) {
			t.Errorf("%s: chapter/volume = %v/%v, want %v/%v", c.name, got.Chapter, got.Volume, c.wantCh, c.wantVol)
		}
	}
}

func floatPtrEqual(a, b *float64) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func targetEqual(a, b *Target) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Status == b.Status && a.Unit == b.Unit && floatPtrEqual(a.Progress, b.Progress) &&
		a.StartDate == b.StartDate && a.FinishDate == b.FinishDate
}

func fmtTarget(t *Target) string {
	if t == nil {
		return "<nil>"
	}
	if t.Progress == nil {
		return string(t.Status) + "/-" + " start=" + t.StartDate + " finish=" + t.FinishDate
	}
	return string(t.Status) + "/" + describeFloat(*t.Progress) + " start=" + t.StartDate + " finish=" + t.FinishDate
}

func TestComputeTargetDates(t *testing.T) {
	ch := core.UnitChapter
	ams, err := time.LoadLocation("Europe/Amsterdam")
	if err != nil {
		t.Fatal(err)
	}
	first := time.Date(2026, 6, 17, 22, 30, 0, 0, time.UTC) // 2026-06-18 00:30 in Amsterdam
	last := time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)
	cases := []struct {
		name       string
		p          core.ReadProgress
		ended      bool
		loc        *time.Location
		wantStart  string
		wantFinish string
	}{
		{"reading sets start only", core.ReadProgress{Unit: ch, BooksTotal: 10, BooksRead: 2, LastReadNumber: 2, FirstReadAt: first, LastReadAt: last},
			false, time.UTC, "2026-06-17", ""},
		{"in progress sets start", core.ReadProgress{Unit: ch, BooksTotal: 10, BooksInProgress: 1, FirstReadAt: first, LastReadAt: last},
			false, time.UTC, "2026-06-17", ""},
		{"all read but ongoing: no finish", core.ReadProgress{Unit: ch, BooksTotal: 3, BooksRead: 3, LastReadNumber: 3, MaxNumber: 3, FirstReadAt: first, LastReadAt: last},
			false, time.UTC, "2026-06-17", ""},
		{"completed sets finish", core.ReadProgress{Unit: ch, BooksTotal: 3, BooksRead: 3, LastReadNumber: 3, MaxNumber: 3, FirstReadAt: first, LastReadAt: last},
			true, time.UTC, "2026-06-17", "2026-08-01"},
		{"time zone shifts the calendar date", core.ReadProgress{Unit: ch, BooksTotal: 10, BooksRead: 1, LastReadNumber: 1, FirstReadAt: first},
			false, ams, "2026-06-18", ""},
		{"unknown dates stay empty", core.ReadProgress{Unit: ch, BooksTotal: 3, BooksRead: 3, LastReadNumber: 3, MaxNumber: 3},
			true, time.UTC, "", ""},
	}
	for _, c := range cases {
		got := ComputeTarget(c.p, c.ended, c.loc)
		if got == nil {
			t.Errorf("%s: got nil", c.name)
			continue
		}
		if got.StartDate != c.wantStart || got.FinishDate != c.wantFinish {
			t.Errorf("%s: dates = %q/%q, want %q/%q", c.name, got.StartDate, got.FinishDate, c.wantStart, c.wantFinish)
		}
	}
	// Nothing read: still nil even if dates are present.
	if got := ComputeTarget(core.ReadProgress{Unit: ch, BooksTotal: 5, FirstReadAt: first}, false, time.UTC); got != nil {
		t.Errorf("nothing read: want nil, got %+v", *got)
	}
}

func TestDecideDates(t *testing.T) {
	reading := Target{Status: core.StatusReading, Progress: f(5), Unit: core.UnitChapter, StartDate: "2026-06-06"}
	readingNoStart := Target{Status: core.StatusReading, Progress: f(5), Unit: core.UnitChapter}
	completed := Target{Status: core.StatusCompleted, Progress: f(80), Unit: core.UnitChapter, StartDate: "2026-06-06", FinishDate: "2026-09-01"}
	// A (hypothetical) reading target that carries a finish date must not write it.
	readingWithFinish := Target{Status: core.StatusReading, Progress: f(5), Unit: core.UnitChapter, FinishDate: "2026-09-01"}

	cases := []struct {
		name       string
		cur        *core.Entry
		target     Target
		wantNil    bool
		wantStart  string
		wantFinish string
		wantStatus bool
		wantCh     *float64
	}{
		{"new entry gets start date", nil, reading, false, "2026-06-06", "", true, f(5)},
		{"new entry without start date", nil, readingNoStart, false, "", "", true, f(5)},
		{"same progress, no start date: only start date", &core.Entry{Status: core.StatusReading, Chapter: f(5)}, reading,
			false, "2026-06-06", "", false, nil},
		{"existing start date is kept", &core.Entry{Status: core.StatusReading, Chapter: f(5), StartDate: "2026-01-01"}, reading,
			true, "", "", false, nil},
		{"existing start date is kept while progress moves", &core.Entry{Status: core.StatusReading, Chapter: f(3), StartDate: "2026-01-01"}, reading,
			false, "", "", false, f(5)},
		{"completed target fills both dates", nil, completed, false, "2026-06-06", "2026-09-01", true, f(80)},
		{"completed target fills finish date only", &core.Entry{Status: core.StatusReading, Chapter: f(79), StartDate: "2026-01-01"}, completed,
			false, "", "2026-09-01", true, f(80)},
		{"existing finish date is kept", &core.Entry{Status: core.StatusReading, Chapter: f(80), StartDate: "2026-01-01", FinishDate: "2026-02-02"},
			Target{Status: core.StatusCompleted, Progress: f(80), Unit: core.UnitChapter, StartDate: "2026-06-06", FinishDate: "2026-09-01"},
			false, "", "", true, nil},
		{"reading target never sets finish date", nil, readingWithFinish, false, "", "", true, f(5)},
		{"reading target never sets finish date on existing entry", &core.Entry{Status: core.StatusReading, Chapter: f(5), StartDate: "2026-01-01"}, readingWithFinish,
			true, "", "", false, nil},
		{"finish before user-set start is skipped", &core.Entry{Status: core.StatusReading, Chapter: f(80), StartDate: "2026-10-01"}, completed,
			false, "", "", true, nil},
		{"finish equal to user-set start is written", &core.Entry{Status: core.StatusReading, Chapter: f(80), StartDate: "2026-09-01"}, completed,
			false, "", "2026-09-01", true, nil},
		{"completed protected despite dates", &core.Entry{Status: core.StatusCompleted}, completed, true, "", "", false, nil},
		{"paused protected despite dates", &core.Entry{Status: core.StatusPaused}, reading, true, "", "", false, nil},
		{"dropped protected despite dates", &core.Entry{Status: core.StatusDropped}, reading, true, "", "", false, nil},
		{"rereading protected despite dates", &core.Entry{Status: core.StatusRereading}, reading, true, "", "", false, nil},
		{"unknown protected despite dates", &core.Entry{Status: core.StatusUnknown}, reading, true, "", "", false, nil},
	}
	for _, c := range cases {
		got := Decide(c.cur, c.target)
		if c.wantNil {
			if got != nil {
				t.Errorf("%s: want nil, got %s", c.name, describe(*got))
			}
			continue
		}
		if got == nil {
			t.Errorf("%s: got nil", c.name)
			continue
		}
		if s := strPtrVal(got.StartDate); s != c.wantStart {
			t.Errorf("%s: start = %q, want %q", c.name, s, c.wantStart)
		}
		if s := strPtrVal(got.FinishDate); s != c.wantFinish {
			t.Errorf("%s: finish = %q, want %q", c.name, s, c.wantFinish)
		}
		if (got.Status != nil) != c.wantStatus {
			t.Errorf("%s: status = %v, want set=%v", c.name, got.Status, c.wantStatus)
		}
		if !floatPtrEqual(got.Chapter, c.wantCh) {
			t.Errorf("%s: chapter = %v, want %v", c.name, got.Chapter, c.wantCh)
		}
	}
}

func TestDescribeDates(t *testing.T) {
	st, fin := "2026-06-06", "2026-09-01"
	got := describe(core.EntryUpdate{StartDate: &st, FinishDate: &fin})
	if got != "start=2026-06-06 finish=2026-09-01" {
		t.Errorf("describe = %q", got)
	}
}

func strPtrVal(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
