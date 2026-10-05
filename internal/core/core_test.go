package core

import (
	"slices"
	"testing"
)

func TestParseStatuses(t *testing.T) {
	got, err := ParseStatuses(" planning, Reading,rereading ,")
	if err != nil {
		t.Fatal(err)
	}
	want := []Status{StatusPlanning, StatusReading, StatusRereading}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	if got, err := ParseStatuses(""); err != nil || got != nil {
		t.Fatalf("empty: got %v, %v; want nil, nil", got, err)
	}
	if _, err := ParseStatuses("planning,bogus"); err == nil {
		t.Fatal("expected error for unknown status")
	}
	if _, err := ParseStatuses("unknown"); err == nil {
		t.Fatal("expected error: 'unknown' is not configurable")
	}
}

func TestSeriesTitles(t *testing.T) {
	s := Series{Title: "Chainsaw Man", AltTitles: []string{"", "Chain Saw Man"}}
	if got := s.Titles(); !slices.Equal(got, []string{"Chainsaw Man", "Chain Saw Man"}) {
		t.Fatalf("got %v", got)
	}
	if got := (Series{}).Titles(); len(got) != 0 {
		t.Fatalf("empty series: got %v", got)
	}
}

func TestReadProgressAllRead(t *testing.T) {
	cases := []struct {
		p    ReadProgress
		want bool
	}{
		{ReadProgress{BooksTotal: 0, BooksRead: 0}, false},
		{ReadProgress{BooksTotal: 10, BooksRead: 9}, false},
		{ReadProgress{BooksTotal: 10, BooksRead: 10}, true},
	}
	for _, c := range cases {
		if got := c.p.AllRead(); got != c.want {
			t.Errorf("%+v: got %v, want %v", c.p, got, c.want)
		}
	}
}
