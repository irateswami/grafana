package schedule

import (
	"context"
	"sync"
	"time"

	"github.com/grafana/grafana/pkg/infra/log"
)

// StarvationDetector monitors organizations for evaluation starvation
type StarvationDetector struct {
	lastEvaluations     map[int64]time.Time
	starvationThreshold time.Duration
	checkInterval       time.Duration
	mu                  sync.RWMutex

	// Metrics and alerting
	starvationCallback func(orgID int64, duration time.Duration)
	logger             log.Logger
}

// NewStarvationDetector creates a new starvation detector
func NewStarvationDetector(threshold, checkInterval time.Duration, callback func(int64, time.Duration), logger log.Logger) *StarvationDetector {
	return &StarvationDetector{
		lastEvaluations:     make(map[int64]time.Time),
		starvationThreshold: threshold,
		checkInterval:       checkInterval,
		starvationCallback:  callback,
		logger:              logger,
	}
}

// Start begins the starvation detection background process
func (sd *StarvationDetector) Start(ctx context.Context) {
	ticker := time.NewTicker(sd.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			sd.checkStarvation()
		case <-ctx.Done():
			return
		}
	}
}

// RecordEvaluation records that an evaluation occurred for an organization
func (sd *StarvationDetector) RecordEvaluation(orgID int64) {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	sd.lastEvaluations[orgID] = time.Now()
}

// checkStarvation checks for organizations that haven't had evaluations recently
func (sd *StarvationDetector) checkStarvation() {
	sd.mu.RLock()
	defer sd.mu.RUnlock()

	now := time.Now()

	for orgID, lastEval := range sd.lastEvaluations {
		starvationDuration := now.Sub(lastEval)

		if starvationDuration > sd.starvationThreshold {
			sd.logger.Warn("Evaluation starvation detected",
				"orgID", orgID,
				"lastEvaluation", lastEval,
				"starvationDuration", starvationDuration,
				"threshold", sd.starvationThreshold)

			if sd.starvationCallback != nil {
				sd.starvationCallback(orgID, starvationDuration)
			}
		}
	}
}

// GetStarvationStatus returns the current starvation status for all organizations
func (sd *StarvationDetector) GetStarvationStatus() map[int64]time.Duration {
	sd.mu.RLock()
	defer sd.mu.RUnlock()

	now := time.Now()
	status := make(map[int64]time.Duration)

	for orgID, lastEval := range sd.lastEvaluations {
		status[orgID] = now.Sub(lastEval)
	}

	return status
}

// RemoveOrganization removes an organization from starvation tracking
func (sd *StarvationDetector) RemoveOrganization(orgID int64) {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	delete(sd.lastEvaluations, orgID)
}

// GetTrackedOrganizations returns the list of organizations being tracked
func (sd *StarvationDetector) GetTrackedOrganizations() []int64 {
	sd.mu.RLock()
	defer sd.mu.RUnlock()

	orgs := make([]int64, 0, len(sd.lastEvaluations))
	for orgID := range sd.lastEvaluations {
		orgs = append(orgs, orgID)
	}

	return orgs
}