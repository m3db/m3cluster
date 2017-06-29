// Package campaign encapsulates the state of a campaign.
package campaign

//go:generate stringer -type State

// State describes the state of a campaign as its relates to the
// caller's leadership.
type State int

const (
	// Follower indicates the caller has called Campaign but has not yet been
	// elected.
	Follower State = iota

	// Leader indicates the caller has called Campaign and was elected.
	Leader

	// Error indicates the call to Campaign returned an error.
	Error

	// Closed indicates the campaign has been closed.
	Closed
)

// Status encapsulates campaign state and any error encountered to
// provide a consistent type for the campaign watch.
type Status struct {
	State State
	Err   error
}

// NewStatus returns a non-error status with the given State.
func NewStatus(s State) Status {
	return Status{State: s}
}

// NewErrCampaignStatus returns an error Status with the given State.
func NewErrCampaignStatus(err error) Status {
	return Status{
		State: Error,
		Err:   err,
	}
}
