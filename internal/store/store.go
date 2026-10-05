// Package store persists sync state in SQLite. Everything in it is a cache that can be
// rebuilt from the external services, so losing the database is safe.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"mangasync/internal/core"

	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS series_map (
	reader        TEXT NOT NULL,
	reader_ref    TEXT NOT NULL,
	tracker       TEXT NOT NULL,
	tracker_id    TEXT NOT NULL DEFAULT '',
	status        TEXT NOT NULL,
	last_attempt  INTEGER NOT NULL DEFAULT 0,
	last_status   TEXT NOT NULL DEFAULT '',
	last_progress REAL,
	PRIMARY KEY (reader, reader_ref, tracker)
);
CREATE TABLE IF NOT EXISTS downloads (
	tracker       TEXT NOT NULL,
	tracker_id    TEXT NOT NULL,
	downloader    TEXT NOT NULL,
	status        TEXT NOT NULL,
	candidate_ref TEXT NOT NULL DEFAULT '',
	source        TEXT NOT NULL DEFAULT '',
	attempts      INTEGER NOT NULL DEFAULT 0,
	retry_after   INTEGER NOT NULL DEFAULT 0,
	updated_at    INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (tracker, tracker_id, downloader)
);`

type MatchStatus string

const (
	Matched   MatchStatus = "matched"
	Unmatched MatchStatus = "unmatched"
)

// SeriesMapping links a reader series to a tracker series.
type SeriesMapping struct {
	Reader       string
	ReaderRef    string
	Tracker      string
	TrackerID    string
	Status       MatchStatus
	LastAttempt  time.Time
	LastStatus   core.Status
	LastProgress *float64
}

type DownloadStatus string

const (
	Acquired   DownloadStatus = "acquired"
	Released   DownloadStatus = "released"
	NotFound   DownloadStatus = "not_found"
	InProgress DownloadStatus = "in_progress"
)

// DownloadRecord is what download sync last did for a tracker series.
type DownloadRecord struct {
	Tracker      string
	TrackerID    string
	Downloader   string
	Status       DownloadStatus
	CandidateRef string
	Source       string
	Attempts     int
	RetryAfter   time.Time
	UpdatedAt    time.Time
}

type Store struct{ db *sql.DB }

// Open opens (creating if needed) the database at path and applies the schema.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	db.SetMaxOpenConns(1) // SQLite has a single writer; one connection also keeps pragmas.
	for _, stmt := range []string{"PRAGMA busy_timeout = 5000", "PRAGMA journal_mode = WAL", schema} {
		if _, err := db.Exec(stmt); err != nil {
			db.Close()
			return nil, fmt.Errorf("init %s: %w", path, err)
		}
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// GetMapping returns nil, nil when no mapping exists.
func (s *Store) GetMapping(ctx context.Context, reader, readerRef, tracker string) (*SeriesMapping, error) {
	m := SeriesMapping{Reader: reader, ReaderRef: readerRef, Tracker: tracker}
	var status, lastStatus string
	var lastAttempt int64
	var lastProgress sql.NullFloat64
	err := s.db.QueryRowContext(ctx,
		`SELECT tracker_id, status, last_attempt, last_status, last_progress
		 FROM series_map WHERE reader = ? AND reader_ref = ? AND tracker = ?`,
		reader, readerRef, tracker).Scan(&m.TrackerID, &status, &lastAttempt, &lastStatus, &lastProgress)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get mapping: %w", err)
	}
	m.Status, m.LastStatus, m.LastAttempt = MatchStatus(status), core.Status(lastStatus), fromUnix(lastAttempt)
	if lastProgress.Valid {
		v := lastProgress.Float64
		m.LastProgress = &v
	}
	return &m, nil
}

func (s *Store) PutMapping(ctx context.Context, m SeriesMapping) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO series_map (reader, reader_ref, tracker, tracker_id, status, last_attempt, last_status, last_progress)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT (reader, reader_ref, tracker) DO UPDATE SET
		   tracker_id = excluded.tracker_id, status = excluded.status, last_attempt = excluded.last_attempt,
		   last_status = excluded.last_status, last_progress = excluded.last_progress`,
		m.Reader, m.ReaderRef, m.Tracker, m.TrackerID, string(m.Status), toUnix(m.LastAttempt),
		string(m.LastStatus), nullFloat(m.LastProgress))
	if err != nil {
		return fmt.Errorf("put mapping: %w", err)
	}
	return nil
}

// MatchedTrackerIDs returns tracker_id -> every reader_ref with a matched mapping to it, between
// reader and tracker. Several reader series can map to one tracker ID (e.g. after a re-import).
func (s *Store) MatchedTrackerIDs(ctx context.Context, reader, tracker string) (map[string][]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT tracker_id, reader_ref FROM series_map
		 WHERE reader = ? AND tracker = ? AND status = ? AND tracker_id <> ''`,
		reader, tracker, string(Matched))
	if err != nil {
		return nil, fmt.Errorf("matched tracker ids: %w", err)
	}
	defer rows.Close()
	out := map[string][]string{}
	for rows.Next() {
		var id, ref string
		if err := rows.Scan(&id, &ref); err != nil {
			return nil, fmt.Errorf("matched tracker ids: %w", err)
		}
		out[id] = append(out[id], ref)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("matched tracker ids: %w", err)
	}
	return out, nil
}

// GetDownload returns nil, nil when no record exists.
func (s *Store) GetDownload(ctx context.Context, tracker, trackerID, downloader string) (*DownloadRecord, error) {
	r := DownloadRecord{Tracker: tracker, TrackerID: trackerID, Downloader: downloader}
	var status string
	var retryAfter, updatedAt int64
	err := s.db.QueryRowContext(ctx,
		`SELECT status, candidate_ref, source, attempts, retry_after, updated_at
		 FROM downloads WHERE tracker = ? AND tracker_id = ? AND downloader = ?`,
		tracker, trackerID, downloader).Scan(&status, &r.CandidateRef, &r.Source, &r.Attempts, &retryAfter, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get download: %w", err)
	}
	r.Status, r.RetryAfter, r.UpdatedAt = DownloadStatus(status), fromUnix(retryAfter), fromUnix(updatedAt)
	return &r, nil
}

func (s *Store) PutDownload(ctx context.Context, r DownloadRecord) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO downloads (tracker, tracker_id, downloader, status, candidate_ref, source, attempts, retry_after, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT (tracker, tracker_id, downloader) DO UPDATE SET
		   status = excluded.status, candidate_ref = excluded.candidate_ref, source = excluded.source,
		   attempts = excluded.attempts, retry_after = excluded.retry_after, updated_at = excluded.updated_at`,
		r.Tracker, r.TrackerID, r.Downloader, string(r.Status), r.CandidateRef, r.Source, r.Attempts,
		toUnix(r.RetryAfter), toUnix(r.UpdatedAt))
	if err != nil {
		return fmt.Errorf("put download: %w", err)
	}
	return nil
}

func toUnix(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}

func fromUnix(v int64) time.Time {
	if v == 0 {
		return time.Time{}
	}
	return time.Unix(v, 0).UTC()
}

func nullFloat(p *float64) sql.NullFloat64 {
	if p == nil {
		return sql.NullFloat64{}
	}
	return sql.NullFloat64{Float64: *p, Valid: true}
}
