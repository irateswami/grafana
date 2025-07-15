package schedule

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/grafana/grafana/pkg/infra/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStarvationDetector_NewStarvationDetector(t *testing.T) {
	threshold := 5 * time.Minute
	checkInterval := 30 * time.Second
	callback := func(int64, time.Duration) {}
	logger := log.NewNopLogger()

	detector := NewStarvationDetector(threshold, checkInterval, callback, logger)

	assert.NotNil(t, detector)
	assert.Equal(t, threshold, detector.starvationThreshold)
	assert.Equal(t, checkInterval, detector.checkInterval)
	assert.NotNil(t, detector.starvationCallback)
	assert.NotNil(t, detector.logger)
	assert.NotNil(t, detector.lastEvaluations)
}

func TestStarvationDetector_RecordEvaluation(t *testing.T) {
	detector := NewStarvationDetector(5*time.Minute, 30*time.Second, nil, log.NewNopLogger())
	
	orgID := int64(1)
	beforeRecord := time.Now()
	
	detector.RecordEvaluation(orgID)
	
	afterRecord := time.Now()
	
	detector.mu.RLock()
	recordedTime, exists := detector.lastEvaluations[orgID]
	detector.mu.RUnlock()
	
	assert.True(t, exists, "Evaluation should be recorded for orgID")
	assert.True(t, recordedTime.After(beforeRecord) || recordedTime.Equal(beforeRecord))
	assert.True(t, recordedTime.Before(afterRecord) || recordedTime.Equal(afterRecord))
}

func TestStarvationDetector_CheckStarvation(t *testing.T) {
	threshold := 100 * time.Millisecond
	var starvationEvents []struct {
		orgID    int64
		duration time.Duration
	}
	var mu sync.Mutex
	
	callback := func(orgID int64, duration time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		starvationEvents = append(starvationEvents, struct {
			orgID    int64
			duration time.Duration
		}{orgID, duration})
	}
	
	detector := NewStarvationDetector(threshold, 10*time.Millisecond, callback, log.NewNopLogger())
	
	// Record an evaluation that should trigger starvation
	orgID := int64(1)
	detector.lastEvaluations[orgID] = time.Now().Add(-200 * time.Millisecond)
	
	// Check for starvation
	detector.checkStarvation()
	
	mu.Lock()
	events := make([]struct {
		orgID    int64
		duration time.Duration
	}, len(starvationEvents))
	copy(events, starvationEvents)
	mu.Unlock()
	
	require.Len(t, events, 1, "Should detect one starvation event")
	assert.Equal(t, orgID, events[0].orgID)
	assert.True(t, events[0].duration >= threshold, "Starvation duration should be at least the threshold")
}

func TestStarvationDetector_CheckStarvation_NoStarvation(t *testing.T) {
	threshold := 5 * time.Minute
	var starvationEvents []struct {
		orgID    int64
		duration time.Duration
	}
	var mu sync.Mutex
	
	callback := func(orgID int64, duration time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		starvationEvents = append(starvationEvents, struct {
			orgID    int64
			duration time.Duration
		}{orgID, duration})
	}
	
	detector := NewStarvationDetector(threshold, 10*time.Millisecond, callback, log.NewNopLogger())
	
	// Record a recent evaluation (should not trigger starvation)
	orgID := int64(1)
	detector.lastEvaluations[orgID] = time.Now().Add(-1 * time.Minute) // Recent evaluation
	
	// Check for starvation
	detector.checkStarvation()
	
	mu.Lock()
	events := make([]struct {
		orgID    int64
		duration time.Duration
	}, len(starvationEvents))
	copy(events, starvationEvents)
	mu.Unlock()
	
	assert.Len(t, events, 0, "Should not detect any starvation events")
}

func TestStarvationDetector_GetStarvationStatus(t *testing.T) {
	detector := NewStarvationDetector(5*time.Minute, 30*time.Second, nil, log.NewNopLogger())
	
	// Record evaluations for multiple organizations
	orgID1 := int64(1)
	orgID2 := int64(2)
	time1 := time.Now().Add(-2 * time.Minute)
	time2 := time.Now().Add(-5 * time.Minute)
	
	detector.lastEvaluations[orgID1] = time1
	detector.lastEvaluations[orgID2] = time2
	
	status := detector.GetStarvationStatus()
	
	require.Len(t, status, 2, "Should return status for both organizations")
	
	// Check that the durations are approximately correct (within 1 second tolerance)
	duration1 := status[orgID1]
	duration2 := status[orgID2]
	
	assert.True(t, duration1 > time.Minute, "Duration for orgID1 should be greater than 1 minute")
	assert.True(t, duration1 < 3*time.Minute, "Duration for orgID1 should be less than 3 minutes")
	
	assert.True(t, duration2 > 4*time.Minute, "Duration for orgID2 should be greater than 4 minutes")
	assert.True(t, duration2 < 6*time.Minute, "Duration for orgID2 should be less than 6 minutes")
}

func TestStarvationDetector_Start(t *testing.T) {
	threshold := 50 * time.Millisecond
	checkInterval := 25 * time.Millisecond
	
	var starvationEvents []struct {
		orgID    int64
		duration time.Duration
	}
	var mu sync.Mutex
	
	callback := func(orgID int64, duration time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		starvationEvents = append(starvationEvents, struct {
			orgID    int64
			duration time.Duration
		}{orgID, duration})
	}
	
	detector := NewStarvationDetector(threshold, checkInterval, callback, log.NewNopLogger())
	
	// Record an old evaluation
	orgID := int64(1)
	detector.lastEvaluations[orgID] = time.Now().Add(-100 * time.Millisecond)
	
	// Start the detector in background
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	
	go detector.Start(ctx)
	
	// Wait for the detection cycle to run
	time.Sleep(100 * time.Millisecond)
	
	mu.Lock()
	eventCount := len(starvationEvents)
	mu.Unlock()
	
	assert.True(t, eventCount > 0, "Should detect starvation events when running periodically")
}

func TestStarvationDetector_RemoveOrganization(t *testing.T) {
	detector := NewStarvationDetector(5*time.Minute, 30*time.Second, nil, log.NewNopLogger())
	
	orgID1 := int64(1)
	orgID2 := int64(2)
	
	// Record evaluations for both organizations
	detector.RecordEvaluation(orgID1)
	detector.RecordEvaluation(orgID2)
	
	// Verify both are tracked
	status := detector.GetStarvationStatus()
	assert.Len(t, status, 2, "Should track both organizations")
	
	// Remove one organization
	detector.RemoveOrganization(orgID1)
	
	// Verify only one is tracked
	status = detector.GetStarvationStatus()
	assert.Len(t, status, 1, "Should track only one organization after removal")
	assert.Contains(t, status, orgID2, "Should still track orgID2")
	assert.NotContains(t, status, orgID1, "Should not track orgID1 after removal")
}

func TestStarvationDetector_GetTrackedOrganizations(t *testing.T) {
	detector := NewStarvationDetector(5*time.Minute, 30*time.Second, nil, log.NewNopLogger())
	
	// Initially should track no organizations
	orgs := detector.GetTrackedOrganizations()
	assert.Len(t, orgs, 0, "Should track no organizations initially")
	
	// Record evaluations for multiple organizations
	detector.RecordEvaluation(1)
	detector.RecordEvaluation(2)
	detector.RecordEvaluation(3)
	
	orgs = detector.GetTrackedOrganizations()
	assert.Len(t, orgs, 3, "Should track three organizations")
	
	// Verify all organizations are present (order doesn't matter)
	orgSet := make(map[int64]bool)
	for _, orgID := range orgs {
		orgSet[orgID] = true
	}
	
	assert.True(t, orgSet[1], "Should track organization 1")
	assert.True(t, orgSet[2], "Should track organization 2")
	assert.True(t, orgSet[3], "Should track organization 3")
}

func TestStarvationDetector_ConcurrentAccess(t *testing.T) {
	detector := NewStarvationDetector(100*time.Millisecond, 10*time.Millisecond, nil, log.NewNopLogger())
	
	var wg sync.WaitGroup
	numGoroutines := 10
	numOperationsPerGoroutine := 100
	
	// Start multiple goroutines that perform concurrent operations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()
			
			orgID := int64(routineID % 3) // Use 3 different org IDs
			
			for j := 0; j < numOperationsPerGoroutine; j++ {
				switch j % 4 {
				case 0:
					detector.RecordEvaluation(orgID)
				case 1:
					detector.GetStarvationStatus()
				case 2:
					detector.GetTrackedOrganizations()
				case 3:
					detector.checkStarvation()
				}
			}
		}(i)
	}
	
	// Wait for all goroutines to complete
	wg.Wait()
	
	// Verify the detector is still functional
	status := detector.GetStarvationStatus()
	assert.True(t, len(status) <= 3, "Should track at most 3 organizations")
}