package leader

//go:generate stringer -type CampaignState

// CampaignState describes the state of a campaign as its relates to the
// caller's leadership.
type CampaignState int

const (
	// CampaignFollower indicates the caller has called Campaign but has not yet
	// been elected.
	CampaignFollower CampaignState = iota

	// CampaignLeader indicates the caller has called Campaign and was elected.
	CampaignLeader

	// CampaignError indicates the call to Campaign returned an error.
	CampaignError

	// CampaignClosed indicates the campaign has been closed.
	CampaignClosed
)

// CampaignStatus encapsulates campaign state and any error encountered to
// provide a consistent type for the campaign watch.
type CampaignStatus struct {
	State CampaignState
	Err   error
}

func okCampaignStatus(s CampaignState) CampaignStatus {
	return CampaignStatus{State: s}
}

func errCampaignStatus(err error) CampaignStatus {
	return CampaignStatus{
		State: CampaignError,
		Err:   err,
	}
}
