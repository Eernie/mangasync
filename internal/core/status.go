package core

import (
	"fmt"
	"slices"
	"strings"
)

// Status is the provider-neutral library status of a series in a tracker.
type Status string

const (
	StatusConsidering Status = "considering"
	StatusPlanning    Status = "planning"
	StatusReading     Status = "reading"
	StatusCompleted   Status = "completed"
	StatusPaused      Status = "paused"
	StatusDropped     Status = "dropped"
	StatusRereading   Status = "rereading"
	StatusUnknown     Status = "unknown"
)

var configurableStatuses = []Status{
	StatusConsidering, StatusPlanning, StatusReading, StatusCompleted,
	StatusPaused, StatusDropped, StatusRereading,
}

// ParseStatuses parses a comma-separated status list. Empty input returns nil.
func ParseStatuses(csv string) ([]Status, error) {
	var out []Status
	for _, part := range strings.Split(csv, ",") {
		s := Status(strings.ToLower(strings.TrimSpace(part)))
		if s == "" {
			continue
		}
		if !slices.Contains(configurableStatuses, s) {
			return nil, fmt.Errorf("unknown status %q (valid: %v)", part, configurableStatuses)
		}
		out = append(out, s)
	}
	return out, nil
}
