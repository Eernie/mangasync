// Package download drives the downloader from tracker library statuses.
package download

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"mangasync/internal/core"
	"mangasync/internal/match"
	"mangasync/internal/store"
)

var notFoundBackoff = []time.Duration{24 * time.Hour, 72 * time.Hour, 168 * time.Hour, 336 * time.Hour, 720 * time.Hour}

// NotFoundBackoff is the wait before searching again after the attempts-th miss (1-based).
func NotFoundBackoff(attempts int) time.Duration {
	i := min(max(attempts, 1), len(notFoundBackoff)) - 1
	return notFoundBackoff[i]
}

// Syncer acquires and releases downloader manga according to the tracker library statuses.
// A record in the store makes every pass idempotent; see the spec's "Download sync" section.
type Syncer struct {
	Reader     core.Reader
	Tracker    core.Tracker
	Lister     core.LibraryLister
	Downloader core.Downloader
	Store      *store.Store
	Acquire    []core.Status
	Release    []core.Status
	Threshold  float64
	DryRun     bool
	Log        *slog.Logger
	Now        func() time.Time // nil = time.Now
}

// Run does one download-sync pass over the tracker library.
func (s *Syncer) Run(ctx context.Context) error {
	statuses := slices.Concat(s.Acquire, s.Release)
	if len(statuses) == 0 {
		return nil
	}
	entries, err := s.Lister.ListLibrary(ctx, statuses)
	if err != nil {
		s.Log.Error("download sync failed", "step", "list tracker library", "err", err)
		return fmt.Errorf("list tracker library: %w", err)
	}
	var readerSeries []core.Series
	if len(s.Acquire) > 0 {
		if readerSeries, err = s.Reader.ListAllSeries(ctx); err != nil {
			s.Log.Error("download sync failed", "step", "list reader series", "err", err)
			return fmt.Errorf("list reader series: %w", err)
		}
	}
	var errs []error
	for _, e := range entries {
		if ctx.Err() != nil {
			return errors.Join(append(errs, ctx.Err())...)
		}
		var err error
		switch {
		case slices.Contains(s.Acquire, e.Status):
			err = s.acquire(ctx, e, readerSeries)
		case slices.Contains(s.Release, e.Status):
			err = s.release(ctx, e)
		}
		if err != nil {
			s.logFor(e).Error("download sync failed", "err", err)
			errs = append(errs, fmt.Errorf("%s: %w", e.Series.Title, err))
		}
	}
	return errors.Join(errs...)
}

func (s *Syncer) acquire(ctx context.Context, e core.LibraryEntry, readerSeries []core.Series) error {
	log := s.logFor(e)
	rec, err := s.Store.GetDownload(ctx, s.Tracker.Name(), e.Series.Ref, s.Downloader.Name())
	if err != nil {
		return err
	}
	if rec != nil {
		if rec.Status == store.Acquired {
			return nil
		}
		if rec.Status == store.NotFound && s.now().Before(rec.RetryAfter) {
			return nil
		}
	}

	// An interrupted acquire may already have put the manga in the downloader library, so
	// FindInLibrary would wrongly report it as done. Resume the acquire instead.
	if rec != nil && rec.Status == store.InProgress && rec.CandidateRef != "" {
		return s.acquireCandidate(ctx, e, core.Candidate{Ref: rec.CandidateRef, SourceName: rec.Source}, "resuming acquire")
	}

	have, err := s.Downloader.FindInLibrary(ctx, e.Series)
	if err != nil {
		return fmt.Errorf("find in downloader library: %w", err)
	}
	if have != nil {
		log.Info("already in downloader library", "candidate", have.Title)
		return s.save(ctx, e, store.Acquired, have, 0, time.Time{})
	}
	// Released files stay on disk and so in the reader. If our own record shows we managed
	// this series in the downloader, the reader copy is ours and must not block re-acquiring.
	managedByUs := rec != nil && rec.CandidateRef != "" &&
		(rec.Status == store.Released || rec.Status == store.InProgress || rec.Status == store.NotFound)
	if !managedByUs {
		for _, rs := range readerSeries {
			if match.SameSeries(rs, e.Series, s.Threshold) {
				log.Debug("already in reader", "reader_series", rs.Title)
				return nil
			}
		}
	}

	best, near, err := s.Downloader.Find(ctx, e.Series)
	if err != nil {
		return fmt.Errorf("search downloader: %w", err)
	}
	if best == nil {
		attempts := 1
		if rec != nil && rec.Status == store.NotFound {
			attempts = rec.Attempts + 1
		}
		retry := s.now().Add(NotFoundBackoff(attempts))
		log.Warn("not found in downloader", "near_misses", describe(near), "attempts", attempts, "retry_after", retry)
		var prev *core.Candidate // keep the managed-by-us marker across misses
		if managedByUs {
			prev = &core.Candidate{Ref: rec.CandidateRef, SourceName: rec.Source}
		}
		return s.save(ctx, e, store.NotFound, prev, attempts, retry)
	}

	return s.acquireCandidate(ctx, e, *best, "acquiring")
}

// acquireCandidate acquires c and records the result: acquired on success, in_progress on
// failure so the next pass resumes. In dry-run mode it only logs.
func (s *Syncer) acquireCandidate(ctx context.Context, e core.LibraryEntry, c core.Candidate, msg string) error {
	s.logFor(e).Info(msg, "candidate", c.Title, "candidate_ref", c.Ref, "source", c.SourceName, "score", c.Score)
	if s.DryRun {
		return nil
	}
	if err := s.Downloader.Acquire(ctx, c); err != nil {
		if serr := s.save(ctx, e, store.InProgress, &c, 0, time.Time{}); serr != nil {
			err = errors.Join(err, serr)
		}
		return fmt.Errorf("acquire: %w", err)
	}
	return s.save(ctx, e, store.Acquired, &c, 0, time.Time{})
}

func (s *Syncer) release(ctx context.Context, e core.LibraryEntry) error {
	log := s.logFor(e)
	rec, err := s.Store.GetDownload(ctx, s.Tracker.Name(), e.Series.Ref, s.Downloader.Name())
	if err != nil {
		return err
	}
	if rec != nil && rec.Status == store.Released {
		return nil
	}
	var prev *core.Candidate // marker of an earlier acquire, kept so the series stays managed by us
	if rec != nil && rec.CandidateRef != "" {
		prev = &core.Candidate{Ref: rec.CandidateRef, SourceName: rec.Source}
	}
	// What we acquired is what we release: title-matching the downloader library could miss it
	// (or hit another manga) and leave the entry behind.
	var have *core.Candidate
	if prev != nil && (rec.Status == store.Acquired || rec.Status == store.InProgress) {
		have = prev
	} else {
		found, err := s.Downloader.FindInLibrary(ctx, e.Series)
		if err != nil {
			return fmt.Errorf("find in downloader library: %w", err)
		}
		have = found
	}
	if have == nil {
		log.Debug("not in downloader library; nothing to release")
		return s.save(ctx, e, store.Released, prev, 0, time.Time{})
	}
	log.Info("releasing", "candidate", have.Title, "candidate_ref", have.Ref, "source", have.SourceName)
	if s.DryRun {
		return nil
	}
	if err := s.Downloader.Release(ctx, *have); err != nil {
		return fmt.Errorf("release: %w", err)
	}
	return s.save(ctx, e, store.Released, have, 0, time.Time{})
}

// save writes the download record. In dry-run mode nothing is recorded.
func (s *Syncer) save(ctx context.Context, e core.LibraryEntry, status store.DownloadStatus, c *core.Candidate, attempts int, retry time.Time) error {
	if s.DryRun {
		return nil
	}
	r := store.DownloadRecord{
		Tracker: s.Tracker.Name(), TrackerID: e.Series.Ref, Downloader: s.Downloader.Name(),
		Status: status, Attempts: attempts, RetryAfter: retry, UpdatedAt: s.now(),
	}
	if c != nil {
		r.CandidateRef, r.Source = c.Ref, c.SourceName
	}
	return s.Store.PutDownload(ctx, r)
}

func (s *Syncer) logFor(e core.LibraryEntry) *slog.Logger {
	return s.Log.With("series", e.Series.Title, "tracker_id", e.Series.Ref, "status", e.Status,
		"downloader", s.Downloader.Name(), "dry_run", s.DryRun)
}

func (s *Syncer) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func describe(cs []core.Candidate) string {
	parts := make([]string, 0, len(cs))
	for _, c := range cs {
		parts = append(parts, fmt.Sprintf("%q (%s, %.2f)", c.Title, c.SourceName, c.Score))
	}
	return strings.Join(parts, "; ")
}
