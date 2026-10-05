package mangabaka

import "mangasync/internal/core"

var stateToStatus = map[string]core.Status{
	"considering":  core.StatusConsidering,
	"plan_to_read": core.StatusPlanning,
	"reading":      core.StatusReading,
	"completed":    core.StatusCompleted,
	"paused":       core.StatusPaused,
	"on_hold":      core.StatusPaused, // reported by third parties, not in the OpenAPI enum
	"dropped":      core.StatusDropped,
	"rereading":    core.StatusRereading,
}

var statusToState = map[core.Status]string{
	core.StatusConsidering: "considering",
	core.StatusPlanning:    "plan_to_read",
	core.StatusReading:     "reading",
	core.StatusCompleted:   "completed",
	core.StatusPaused:      "paused",
	core.StatusDropped:     "dropped",
	core.StatusRereading:   "rereading",
}

func statusFromState(state string) core.Status {
	if s, ok := stateToStatus[state]; ok {
		return s
	}
	return core.StatusUnknown
}

func stateFromStatus(s core.Status) (string, bool) {
	state, ok := statusToState[s]
	return state, ok
}
