package progress

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"mangasync/internal/core"
	"mangasync/internal/store"
)

const defaultUnmatchedRetry = 24 * time.Hour

// Syncer pushes one reader series' progress to every tracker.
type Syncer struct {
	Reader         core.Reader
	Trackers       []core.Tracker
	Store          *store.Store
	DryRun         bool
	Log            *slog.Logger
	Now            func() time.Time // nil = time.Now
	UnmatchedRetry time.Duration    // 0 = 24h
}

// SyncSeries syncs one reader series. A failing tracker is logged and does not stop the others;
// all tracker errors are returned joined.
func (s *Syncer) SyncSeries(ctx context.Context, ref string) error {
	series, err := s.Reader.GetSeries(ctx, ref)
	if err != nil {
		s.Log.Error("progress sync failed", "ref", ref, "step", "get series", "err", err)
		return fmt.Errorf("get series %s: %w", ref, err)
	}
	prog, err := s.Reader.GetProgress(ctx, ref)
	if err != nil {
		s.Log.Error("progress sync failed", "ref", ref, "step", "get progress", "err", err)
		return fmt.Errorf("get progress %s: %w", ref, err)
	}
	var errs []error
	for _, tr := range s.Trackers {
		if ctx.Err() != nil {
			return errors.Join(append(errs, ctx.Err())...)
		}
		if err := s.syncTracker(ctx, series, prog, tr); err != nil {
			s.Log.Error("progress sync failed", "series", series.Title, "tracker", tr.Name(), "err", err)
			errs = append(errs, fmt.Errorf("%s: %w", tr.Name(), err))
		}
	}
	return errors.Join(errs...)
}

func (s *Syncer) syncTracker(ctx context.Context, series core.Series, prog core.ReadProgress, tr core.Tracker) error {
	id, ok, err := s.resolve(ctx, series, tr)
	if err != nil {
		return fmt.Errorf("resolve: %w", err)
	}
	if !ok {
		return nil
	}
	ended := false
	if prog.AllRead() {
		if ended, err = tr.SeriesEnded(ctx, id); err != nil {
			return fmt.Errorf("series ended: %w", err)
		}
	}
	target := ComputeTarget(prog, ended, nil)
	if target == nil {
		return nil
	}
	cur, err := tr.GetEntry(ctx, id)
	if err != nil {
		return fmt.Errorf("get entry: %w", err)
	}
	log := s.Log.With("series", series.Title, "tracker", tr.Name(), "tracker_id", id, "dry_run", s.DryRun)
	upd := Decide(cur, *target)
	if upd == nil {
		log.Debug("tracker entry up to date")
		return nil
	}
	log.Info("updating tracker entry", "update", describe(*upd))
	if s.DryRun {
		return nil
	}
	if err := tr.SaveEntry(ctx, id, *upd); err != nil {
		return fmt.Errorf("save entry: %w", err)
	}
	status, progress := pushedState(cur, *upd, target.Unit)
	return s.Store.PutMapping(ctx, store.SeriesMapping{
		Reader: s.Reader.Name(), ReaderRef: series.Ref, Tracker: tr.Name(), TrackerID: id,
		Status: store.Matched, LastAttempt: s.now(), LastStatus: status, LastProgress: progress,
	})
}

// pushedState is the tracker's status and progress (for unit) after applying upd to cur:
// what was actually pushed, which can differ from the target when the tracker was already ahead.
func pushedState(cur *core.Entry, upd core.EntryUpdate, unit core.Unit) (core.Status, *float64) {
	var status core.Status
	var progress *float64
	if cur != nil {
		status = cur.Status
		progress = cur.Chapter
		if unit == core.UnitVolume {
			progress = cur.Volume
		}
	}
	if upd.Status != nil {
		status = *upd.Status
	}
	pushed := upd.Chapter
	if unit == core.UnitVolume {
		pushed = upd.Volume
	}
	if pushed != nil {
		progress = pushed
	}
	return status, progress
}

// resolve returns the cached tracker ID, or resolves and caches it. Unmatched series are
// retried at most once per UnmatchedRetry to protect search rate limits.
func (s *Syncer) resolve(ctx context.Context, series core.Series, tr core.Tracker) (string, bool, error) {
	m, err := s.Store.GetMapping(ctx, s.Reader.Name(), series.Ref, tr.Name())
	if err != nil {
		return "", false, err
	}
	if m != nil && m.Status == store.Matched {
		return m.TrackerID, true, nil
	}
	if m != nil && m.Status == store.Unmatched && s.now().Sub(m.LastAttempt) < s.unmatchedRetry() {
		return "", false, nil
	}
	id, found, err := tr.Resolve(ctx, series)
	if err != nil {
		return "", false, err
	}
	nm := store.SeriesMapping{Reader: s.Reader.Name(), ReaderRef: series.Ref, Tracker: tr.Name(), LastAttempt: s.now()}
	if found {
		nm.Status, nm.TrackerID = store.Matched, id
		s.Log.Info("matched series", "series", series.Title, "tracker", tr.Name(), "tracker_id", id)
	} else {
		nm.Status = store.Unmatched
		s.Log.Warn("no tracker match; retrying later", "series", series.Title, "tracker", tr.Name(), "retry_in", s.unmatchedRetry())
	}
	if err := s.Store.PutMapping(ctx, nm); err != nil {
		return "", false, err
	}
	return id, found, nil
}

func (s *Syncer) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *Syncer) unmatchedRetry() time.Duration {
	if s.UnmatchedRetry > 0 {
		return s.UnmatchedRetry
	}
	return defaultUnmatchedRetry
}
