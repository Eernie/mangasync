package progress

import (
	"testing"

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
		got := ComputeTarget(c.p, c.ended)
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
	return a.Status == b.Status && a.Unit == b.Unit && floatPtrEqual(a.Progress, b.Progress)
}

func fmtTarget(t *Target) string {
	if t == nil {
		return "<nil>"
	}
	if t.Progress == nil {
		return string(t.Status) + "/-"
	}
	return string(t.Status) + "/" + describeFloat(*t.Progress)
}
