package semaphore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOrgSemaphoreManager_NewOrgSemaphoreManager(t *testing.T) {
	globalLimit := 10
	perOrgLimit := 5
	enabled := true

	osm := NewOrgSemaphoreManager(globalLimit, perOrgLimit, enabled)

	assert.NotNil(t, osm)
	assert.Equal(t, globalLimit, osm.globalLimit)
	assert.Equal(t, perOrgLimit, osm.perOrgLimit)
	assert.Equal(t, enabled, osm.enabled)
	assert.Equal(t, globalLimit, cap(osm.globalSemaphore))
	assert.Empty(t, osm.orgSemaphores)

	// Cleanup
	osm.Stop()
}

func TestOrgSemaphoreManager_TryAcquireEvaluationSlot_Success(t *testing.T) {
	osm := NewOrgSemaphoreManager(10, 5, true)
	defer osm.Stop()

	ctx := context.Background()
	orgID := int64(1)

	releaseFunc, err := osm.TryAcquireEvaluationSlot(ctx, orgID)
	require.NoError(t, err)
	require.NotNil(t, releaseFunc)

	// Verify that semaphores are acquired
	assert.Equal(t, 1, len(osm.globalSemaphore))
	
	osm.mu.RLock()
	orgSem, exists := osm.orgSemaphores[orgID]
	osm.mu.RUnlock()
	require.True(t, exists)
	assert.Equal(t, 1, len(orgSem.semaphore))

	// Release and verify
	releaseFunc()
	assert.Equal(t, 0, len(osm.globalSemaphore))
	assert.Equal(t, 0, len(orgSem.semaphore))
}

func TestOrgSemaphoreManager_TryAcquireEvaluationSlot_GlobalLimit(t *testing.T) {
	globalLimit := 2
	osm := NewOrgSemaphoreManager(globalLimit, 5, true)
	defer osm.Stop()

	ctx := context.Background()
	orgID := int64(1)

	// Acquire all global slots
	var releases []func()
	for i := 0; i < globalLimit; i++ {
		releaseFunc, err := osm.TryAcquireEvaluationSlot(ctx, orgID)
		require.NoError(t, err)
		releases = append(releases, releaseFunc)
	}

	// Next acquisition should fail due to global limit
	_, err := osm.TryAcquireEvaluationSlot(ctx, orgID)
	assert.ErrorIs(t, err, ErrGlobalConcurrencyLimitReached)

	// Release one slot and try again
	releases[0]()
	releaseFunc, err := osm.TryAcquireEvaluationSlot(ctx, orgID)
	require.NoError(t, err)
	
	// Cleanup
	releaseFunc()
	for _, release := range releases[1:] {
		release()
	}
}

func TestOrgSemaphoreManager_TryAcquireEvaluationSlot_PerOrgLimit(t *testing.T) {
	perOrgLimit := 2
	osm := NewOrgSemaphoreManager(10, perOrgLimit, true)
	defer osm.Stop()

	ctx := context.Background()
	orgID := int64(1)

	// Acquire all per-org slots
	var releases []func()
	for i := 0; i < perOrgLimit; i++ {
		releaseFunc, err := osm.TryAcquireEvaluationSlot(ctx, orgID)
		require.NoError(t, err)
		releases = append(releases, releaseFunc)
	}

	// Next acquisition should fail due to per-org limit
	_, err := osm.TryAcquireEvaluationSlot(ctx, orgID)
	assert.ErrorIs(t, err, ErrOrgConcurrencyLimitReached)

	// Different org should still work
	otherOrgID := int64(2)
	releaseFunc, err := osm.TryAcquireEvaluationSlot(ctx, otherOrgID)
	require.NoError(t, err)
	releaseFunc()

	// Cleanup
	for _, release := range releases {
		release()
	}
}

func TestOrgSemaphoreManager_TryAcquireEvaluationSlot_ContextCanceled(t *testing.T) {
	osm := NewOrgSemaphoreManager(1, 1, true)
	defer osm.Stop()

	orgID := int64(1)

	// Acquire the only slot
	releaseFunc, err := osm.TryAcquireEvaluationSlot(context.Background(), orgID)
	require.NoError(t, err)

	// Try to acquire with canceled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = osm.TryAcquireEvaluationSlot(ctx, orgID)
	assert.ErrorIs(t, err, context.Canceled)

	// Cleanup
	releaseFunc()
}

func TestOrgSemaphoreManager_TryAcquireEvaluationSlot_Disabled(t *testing.T) {
	osm := NewOrgSemaphoreManager(10, 5, false) // disabled
	defer osm.Stop()

	ctx := context.Background()
	orgID := int64(1)

	releaseFunc, err := osm.TryAcquireEvaluationSlot(ctx, orgID)
	require.NoError(t, err)
	require.NotNil(t, releaseFunc)

	// When disabled, no actual semaphore usage
	assert.Equal(t, 0, len(osm.globalSemaphore))
	assert.Empty(t, osm.orgSemaphores)

	// Release should be safe to call
	releaseFunc()
}

func TestOrgSemaphoreManager_UpdateLimits(t *testing.T) {
	osm := NewOrgSemaphoreManager(5, 3, true)
	defer osm.Stop()

	ctx := context.Background()
	orgID := int64(1)

	// Acquire some slots
	releaseFunc1, err := osm.TryAcquireEvaluationSlot(ctx, orgID)
	require.NoError(t, err)
	releaseFunc2, err := osm.TryAcquireEvaluationSlot(ctx, orgID)
	require.NoError(t, err)

	// Update limits
	newGlobalLimit := 10
	newPerOrgLimit := 2

	osm.UpdateLimits(newGlobalLimit, newPerOrgLimit)

	assert.Equal(t, newGlobalLimit, osm.globalLimit)
	assert.Equal(t, newPerOrgLimit, osm.perOrgLimit)
	assert.Equal(t, newGlobalLimit, cap(osm.globalSemaphore))

	osm.mu.RLock()
	orgSem := osm.orgSemaphores[orgID]
	osm.mu.RUnlock()
	assert.Equal(t, newPerOrgLimit, cap(orgSem.semaphore))

	// Original slots should still be active
	assert.Equal(t, 2, len(osm.globalSemaphore))
	assert.Equal(t, 2, len(orgSem.semaphore))

	// Can't acquire more due to new lower per-org limit
	_, err = osm.TryAcquireEvaluationSlot(ctx, orgID)
	assert.ErrorIs(t, err, ErrOrgConcurrencyLimitReached)

	// Cleanup
	releaseFunc1()
	releaseFunc2()
}

func TestOrgSemaphoreManager_GetRichStats(t *testing.T) {
	osm := NewOrgSemaphoreManager(10, 5, true)
	defer osm.Stop()

	ctx := context.Background()
	orgID := int64(1)

	// Acquire some slots
	releaseFunc, err := osm.TryAcquireEvaluationSlot(ctx, orgID)
	require.NoError(t, err)

	stats := osm.GetRichStats()

	assert.Equal(t, 10, stats["global_limit"])
	assert.Equal(t, 1, stats["global_active"])
	assert.Equal(t, 5, stats["per_org_limit"])
	assert.Equal(t, true, stats["enabled"])
	assert.Equal(t, 1, stats["org_semaphores"])

	orgDetails, ok := stats["org_details"].(map[int64]map[string]interface{})
	require.True(t, ok)
	
	orgStats, exists := orgDetails[orgID]
	require.True(t, exists)
	assert.Equal(t, 1, orgStats["active"])
	assert.Contains(t, orgStats, "last_accessed")

	// Cleanup
	releaseFunc()
}

func TestOrgSemaphoreManager_MetricsTracking(t *testing.T) {
	osm := NewOrgSemaphoreManager(10, 1, true) // Higher global limit to ensure we hit per-org limit
	defer osm.Stop()

	ctx := context.Background()
	orgID := int64(1)

	// Acquire the only per-org slot
	releaseFunc, err := osm.TryAcquireEvaluationSlot(ctx, orgID)
	require.NoError(t, err)

	// Try to acquire another - should be dropped due to per-org limit
	_, err = osm.TryAcquireEvaluationSlot(ctx, orgID)
	assert.ErrorIs(t, err, ErrOrgConcurrencyLimitReached)

	// Check metrics
	assert.Equal(t, int64(1), osm.metrics.DroppedEvaluations[orgID])
	assert.Contains(t, osm.metrics.WaitDurations, orgID)

	// Release and check completion metrics
	releaseFunc()
	assert.Contains(t, osm.metrics.LastEvalTimes, orgID)
}

func TestOrgSemaphoreManager_GarbageCollection(t *testing.T) {
	// Use short intervals for testing
	osm := NewOrgSemaphoreManager(10, 5, true)
	osm.gcInterval = 10 * time.Millisecond
	osm.semaphoreIdleTime = 20 * time.Millisecond
	defer osm.Stop()

	ctx := context.Background()
	orgID := int64(1)

	// Create a semaphore for the org
	releaseFunc, err := osm.TryAcquireEvaluationSlot(ctx, orgID)
	require.NoError(t, err)
	releaseFunc()

	// Initially, semaphore should exist
	osm.mu.RLock()
	_, exists := osm.orgSemaphores[orgID]
	osm.mu.RUnlock()
	assert.True(t, exists)

	// Wait for GC to run and clean up idle semaphore
	time.Sleep(50 * time.Millisecond)

	osm.mu.RLock()
	_, exists = osm.orgSemaphores[orgID]
	osm.mu.RUnlock()
	assert.False(t, exists, "Semaphore should have been garbage collected")
}

func TestOrgSemaphoreManager_AcquireEvaluationSlot_Legacy(t *testing.T) {
	osm := NewOrgSemaphoreManager(10, 5, true)
	defer osm.Stop()

	orgID := int64(1)

	releaseFunc, err := osm.AcquireEvaluationSlot(orgID)
	require.NoError(t, err)
	require.NotNil(t, releaseFunc)

	// Verify that semaphores are acquired
	assert.Equal(t, 1, len(osm.globalSemaphore))
	
	osm.mu.RLock()
	orgSem, exists := osm.orgSemaphores[orgID]
	osm.mu.RUnlock()
	require.True(t, exists)
	assert.Equal(t, 1, len(orgSem.semaphore))

	// Release and verify
	releaseFunc()
	assert.Equal(t, 0, len(osm.globalSemaphore))
	assert.Equal(t, 0, len(orgSem.semaphore))
}

func TestOrgSemaphoreManager_MultipleOrganizations(t *testing.T) {
	osm := NewOrgSemaphoreManager(10, 2, true)
	defer osm.Stop()

	ctx := context.Background()
	
	// Test with multiple organizations
	orgs := []int64{1, 2, 3}
	var releases []func()

	// Each org should be able to acquire up to their limit
	for _, orgID := range orgs {
		for i := 0; i < 2; i++ {
			releaseFunc, err := osm.TryAcquireEvaluationSlot(ctx, orgID)
			require.NoError(t, err)
			releases = append(releases, releaseFunc)
		}
	}

	// Total active should be 6 (3 orgs * 2 per org)
	assert.Equal(t, 6, len(osm.globalSemaphore))

	// Each org should be at limit
	for _, orgID := range orgs {
		_, err := osm.TryAcquireEvaluationSlot(ctx, orgID)
		assert.ErrorIs(t, err, ErrOrgConcurrencyLimitReached)
	}

	// Cleanup
	for _, release := range releases {
		release()
	}
}