package schedule

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/grafana/pkg/services/ngalert/semaphore"
)

func TestScheduler_SemaphoreIntegration(t *testing.T) {
	t.Run("semaphore manager creation", func(t *testing.T) {
		// Test that semaphore manager can be created with the configuration
		manager := semaphore.NewOrgSemaphoreManager(5, 2, true)
		require.NotNil(t, manager)
		
		stats := manager.GetRichStats()
		require.Equal(t, 5, stats["global_limit"])
		require.Equal(t, 2, stats["per_org_limit"])
		require.Equal(t, true, stats["enabled"])
	})

	t.Run("semaphore acquisition and release", func(t *testing.T) {
		manager := semaphore.NewOrgSemaphoreManager(5, 2, true)
		ctx := context.Background()
		orgID := int64(1)
		
		// Test successful acquisition
		releaseSlot, err := manager.TryAcquireEvaluationSlot(ctx, orgID)
		require.NoError(t, err)
		require.NotNil(t, releaseSlot)
		
		// Release the slot
		releaseSlot()
		
		// Test that acquisition still works after release
		releaseSlot2, err := manager.TryAcquireEvaluationSlot(ctx, orgID)
		require.NoError(t, err)
		require.NotNil(t, releaseSlot2)
		releaseSlot2()
	})

	t.Run("concurrency limits are enforced", func(t *testing.T) {
		manager := semaphore.NewOrgSemaphoreManager(5, 2, true)
		ctx := context.Background()
		orgID := int64(1)
		
		// Acquire all available slots for this org (limit is 2)
		release1, err := manager.TryAcquireEvaluationSlot(ctx, orgID)
		require.NoError(t, err)
		
		release2, err := manager.TryAcquireEvaluationSlot(ctx, orgID)
		require.NoError(t, err)
		
		// Third acquisition should fail
		_, err = manager.TryAcquireEvaluationSlot(ctx, orgID)
		require.Error(t, err)
		require.Contains(t, err.Error(), "organization evaluation concurrency limit reached")
		
		// Release and try again
		release1()
		release3, err := manager.TryAcquireEvaluationSlot(ctx, orgID)
		require.NoError(t, err)
		
		// Clean up
		release2()
		release3()
	})

	t.Run("context timeout is respected", func(t *testing.T) {
		manager := semaphore.NewOrgSemaphoreManager(1, 1, true) // Very low limits
		ctx := context.Background()
		orgID := int64(1)
		
		// Fill up the semaphore
		release1, err := manager.TryAcquireEvaluationSlot(ctx, orgID)
		require.NoError(t, err)
		defer release1()
		
		// Now try with a timeout context - should fail since semaphore is full
		timeoutCtx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
		defer cancel()
		
		_, err = manager.TryAcquireEvaluationSlot(timeoutCtx, orgID)
		require.Error(t, err)
		// Just check that we get an error - exact error depends on implementation details
		t.Logf("Got error: %v", err)
	})

	t.Run("disabled mode works correctly", func(t *testing.T) {
		manager := semaphore.NewOrgSemaphoreManager(5, 2, false) // disabled
		ctx := context.Background()
		orgID := int64(1)
		
		// Should always succeed when disabled
		releaseSlot, err := manager.TryAcquireEvaluationSlot(ctx, orgID)
		require.NoError(t, err)
		require.NotNil(t, releaseSlot)
		releaseSlot()
	})
}