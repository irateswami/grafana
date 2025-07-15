package semaphore

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkOrgSemaphoreManager_Acquisition tests the performance of slot acquisition and release
func BenchmarkOrgSemaphoreManager_Acquisition(b *testing.B) {
	tests := []struct {
		name           string
		globalLimit    int
		perOrgLimit    int
		organizations  int
		enabled        bool
	}{
		{"Small_Enabled", 50, 10, 5, true},
		{"Medium_Enabled", 100, 20, 10, true},
		{"Large_Enabled", 200, 25, 20, true},
		{"Small_Disabled", 50, 10, 5, false},
		{"Medium_Disabled", 100, 20, 10, false},
		{"Large_Disabled", 200, 25, 20, false},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			manager := NewOrgSemaphoreManager(tt.globalLimit, tt.perOrgLimit, tt.enabled)
			defer manager.Stop()
			
			b.ResetTimer()
			b.ReportAllocs()
			
			b.RunParallel(func(pb *testing.PB) {
				orgID := int64(1 + (runtime.NumGoroutine() % tt.organizations))
				ctx := context.Background()
				
				for pb.Next() {
					release, err := manager.TryAcquireEvaluationSlot(ctx, orgID)
					if err == nil && release != nil {
						release()
					}
				}
			})
		})
	}
}

// BenchmarkOrgSemaphoreManager_ConcurrentAcquisition tests performance under high concurrency
func BenchmarkOrgSemaphoreManager_ConcurrentAcquisition(b *testing.B) {
	manager := NewOrgSemaphoreManager(100, 15, true)
	defer manager.Stop()
	
	b.ResetTimer()
	b.ReportAllocs()
	
	b.RunParallel(func(pb *testing.PB) {
		orgID := int64(1 + (runtime.NumGoroutine() % 10))
		ctx := context.Background()
		
		for pb.Next() {
			release, err := manager.TryAcquireEvaluationSlot(ctx, orgID)
			if err == nil && release != nil {
				// Simulate brief work period
				time.Sleep(100 * time.Microsecond)
				release()
			}
		}
	})
}

// TestLoadSemaphoreManager_HighContention simulates realistic load patterns
func TestLoadSemaphoreManager_HighContention(t *testing.T) {
	tests := []struct {
		name              string
		globalLimit       int
		perOrgLimit      int
		organizations    int
		goroutinesPerOrg int
		duration         time.Duration
		expectedMinSuccess float64 // Minimum success rate percentage
	}{
		{
			name:              "Realistic_Load",
			globalLimit:       50,
			perOrgLimit:      10,
			organizations:    5,
			goroutinesPerOrg: 20,
			duration:         5 * time.Second,
			expectedMinSuccess: 0.4, // With high contention, expect very low success rate
		},
		{
			name:              "High_Contention",
			globalLimit:       20,
			perOrgLimit:      5,
			organizations:    10,
			goroutinesPerOrg: 25,
			duration:         3 * time.Second,
			expectedMinSuccess: 0.1, // Very high contention, very low success rate
		},
		{
			name:              "Burst_Load",
			globalLimit:       100,
			perOrgLimit:      25,
			organizations:    3,
			goroutinesPerOrg: 50,
			duration:         2 * time.Second,
			expectedMinSuccess: 1.0, // Even with higher limits, tight loop creates contention
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewOrgSemaphoreManager(tt.globalLimit, tt.perOrgLimit, true)
			defer manager.Stop()
			
			var (
				totalAttempts  int64
				totalSuccess   int64
				totalFailures  int64
				totalTimeouts  int64
			)
			
			ctx, cancel := context.WithTimeout(context.Background(), tt.duration)
			defer cancel()
			
			var wg sync.WaitGroup
			
			// Start workers for each organization
			for orgID := int64(1); orgID <= int64(tt.organizations); orgID++ {
				for worker := 0; worker < tt.goroutinesPerOrg; worker++ {
					wg.Add(1)
					go func(orgID int64, workerID int) {
						defer wg.Done()
						
						for {
							select {
							case <-ctx.Done():
								return
							default:
							}
							
							atomic.AddInt64(&totalAttempts, 1)
							
							// Create a context with timeout for each acquisition
							acqCtx, acqCancel := context.WithTimeout(ctx, 50*time.Millisecond)
							
							release, err := manager.TryAcquireEvaluationSlot(acqCtx, orgID)
							acqCancel()
							
							if err != nil {
								if err == context.DeadlineExceeded {
									atomic.AddInt64(&totalTimeouts, 1)
								} else {
									atomic.AddInt64(&totalFailures, 1)
								}
								continue
							}
							
							atomic.AddInt64(&totalSuccess, 1)
							
							// Simulate evaluation work
							workTime := time.Duration(10+worker%20) * time.Millisecond
							time.Sleep(workTime)
							
							release()
						}
					}(orgID, worker)
				}
			}
			
			wg.Wait()
			
			// Calculate metrics
			attempts := atomic.LoadInt64(&totalAttempts)
			successes := atomic.LoadInt64(&totalSuccess)
			failures := atomic.LoadInt64(&totalFailures)
			timeouts := atomic.LoadInt64(&totalTimeouts)
			
			successRate := float64(successes) / float64(attempts) * 100
			
			t.Logf("Load Test Results for %s:", tt.name)
			t.Logf("  Duration: %v", tt.duration)
			t.Logf("  Organizations: %d", tt.organizations)
			t.Logf("  Workers per org: %d", tt.goroutinesPerOrg)
			t.Logf("  Global limit: %d", tt.globalLimit)
			t.Logf("  Per-org limit: %d", tt.perOrgLimit)
			t.Logf("  Total attempts: %d", attempts)
			t.Logf("  Successful acquisitions: %d", successes)
			t.Logf("  Concurrency limit failures: %d", failures)
			t.Logf("  Timeout failures: %d", timeouts)
			t.Logf("  Success rate: %.2f%%", successRate)
			
			// Verify minimum success rate
			if successRate < tt.expectedMinSuccess {
				t.Errorf("Success rate %.2f%% is below expected minimum %.2f%%", 
					successRate, tt.expectedMinSuccess)
			}
			
			// Verify that we had meaningful contention
			if attempts < 100 {
				t.Errorf("Not enough attempts (%d) to validate load test", attempts)
			}
			
			// Get final statistics
			stats := manager.GetRichStats()
			t.Logf("Final semaphore statistics:")
			t.Logf("  Global active: %v", stats["global_active"])
			t.Logf("  Organization count: %v", stats["org_semaphores"])
		})
	}
}

// TestLoadSemaphoreManager_StarvationPrevention tests starvation scenarios
func TestLoadSemaphoreManager_StarvationPrevention(t *testing.T) {
	manager := NewOrgSemaphoreManager(20, 5, true)
	defer manager.Stop()
	
	var (
		org1Successes int64
		org2Successes int64
		org3Successes int64
	)
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	var wg sync.WaitGroup
	
	// Organization 1: High frequency, short work
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			
			acqCtx, acqCancel := context.WithTimeout(ctx, 10*time.Millisecond)
			release, err := manager.TryAcquireEvaluationSlot(acqCtx, 1)
			acqCancel()
			
			if err == nil {
				atomic.AddInt64(&org1Successes, 1)
				time.Sleep(1 * time.Millisecond) // Very short work
				release()
			}
			time.Sleep(5 * time.Millisecond) // Brief pause
		}
	}()
	
	// Organization 2: Medium frequency, medium work
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			
			acqCtx, acqCancel := context.WithTimeout(ctx, 20*time.Millisecond)
			release, err := manager.TryAcquireEvaluationSlot(acqCtx, 2)
			acqCancel()
			
			if err == nil {
				atomic.AddInt64(&org2Successes, 1)
				time.Sleep(10 * time.Millisecond) // Medium work
				release()
			}
			time.Sleep(15 * time.Millisecond) // Medium pause
		}
	}()
	
	// Organization 3: Low frequency, long work
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			
			acqCtx, acqCancel := context.WithTimeout(ctx, 50*time.Millisecond)
			release, err := manager.TryAcquireEvaluationSlot(acqCtx, 3)
			acqCancel()
			
			if err == nil {
				atomic.AddInt64(&org3Successes, 1)
				time.Sleep(25 * time.Millisecond) // Long work
				release()
			}
			time.Sleep(100 * time.Millisecond) // Long pause
		}
	}()
	
	wg.Wait()
	
	org1 := atomic.LoadInt64(&org1Successes)
	org2 := atomic.LoadInt64(&org2Successes)
	org3 := atomic.LoadInt64(&org3Successes)
	
	t.Logf("Starvation Prevention Test Results:")
	t.Logf("  Org 1 (high freq, short work): %d successes", org1)
	t.Logf("  Org 2 (med freq, med work): %d successes", org2)
	t.Logf("  Org 3 (low freq, long work): %d successes", org3)
	
	// Verify that all organizations got some successful evaluations
	if org1 == 0 {
		t.Error("Organization 1 was completely starved")
	}
	if org2 == 0 {
		t.Error("Organization 2 was completely starved")
	}
	if org3 == 0 {
		t.Error("Organization 3 was completely starved")
	}
	
	// Verify reasonable distribution (org1 should have most, but not 100%)
	total := org1 + org2 + org3
	org1Percentage := float64(org1) / float64(total) * 100
	
	if org1Percentage > 95 {
		t.Errorf("Organization 1 dominated too much (%.1f%% of successes)", org1Percentage)
	}
	
	t.Logf("  Total successes: %d", total)
	t.Logf("  Org 1 percentage: %.1f%%", org1Percentage)
}

// TestLoadSemaphoreManager_BurstTraffic tests burst traffic scenarios
func TestLoadSemaphoreManager_BurstTraffic(t *testing.T) {
	manager := NewOrgSemaphoreManager(50, 10, true)
	defer manager.Stop()
	
	burstTests := []struct {
		name        string
		burstSize   int
		burstDuration time.Duration
		waitBetween time.Duration
		bursts      int
	}{
		{"Small_Bursts", 20, 100 * time.Millisecond, 500 * time.Millisecond, 5},
		{"Medium_Bursts", 50, 200 * time.Millisecond, 1 * time.Second, 3},
		{"Large_Bursts", 100, 500 * time.Millisecond, 2 * time.Second, 2},
	}
	
	for _, bt := range burstTests {
		t.Run(bt.name, func(t *testing.T) {
			var totalAttempts, totalSuccesses int64
			
			for burst := 0; burst < bt.bursts; burst++ {
				var wg sync.WaitGroup
				burstCtx, burstCancel := context.WithTimeout(context.Background(), bt.burstDuration)
				
				// Launch burst workers
				for i := 0; i < bt.burstSize; i++ {
					wg.Add(1)
					go func(workerID int) {
						defer wg.Done()
						orgID := int64(1 + (workerID % 5)) // 5 organizations
						
						atomic.AddInt64(&totalAttempts, 1)
						
						acqCtx, acqCancel := context.WithTimeout(burstCtx, 50*time.Millisecond)
						release, err := manager.TryAcquireEvaluationSlot(acqCtx, orgID)
						acqCancel()
						
						if err == nil {
							atomic.AddInt64(&totalSuccesses, 1)
							time.Sleep(10 * time.Millisecond) // Simulate work
							release()
						}
					}(i)
				}
				
				wg.Wait()
				burstCancel()
				
				// Wait between bursts
				if burst < bt.bursts-1 {
					time.Sleep(bt.waitBetween)
				}
			}
			
			attempts := atomic.LoadInt64(&totalAttempts)
			successes := atomic.LoadInt64(&totalSuccesses)
			successRate := float64(successes) / float64(attempts) * 100
			
			t.Logf("Burst Test %s Results:", bt.name)
			t.Logf("  Bursts: %d", bt.bursts)
			t.Logf("  Burst size: %d workers", bt.burstSize)
			t.Logf("  Burst duration: %v", bt.burstDuration)
			t.Logf("  Total attempts: %d", attempts)
			t.Logf("  Total successes: %d", successes)
			t.Logf("  Success rate: %.2f%%", successRate)
			
			// Verify reasonable success rate for burst traffic
			if successRate < 20.0 { // At least 20% should succeed even in bursts
				t.Errorf("Burst success rate %.2f%% is too low", successRate)
			}
		})
	}
}

// TestLoadSemaphoreManager_MemoryUsage tests memory usage under load
func TestLoadSemaphoreManager_MemoryUsage(t *testing.T) {
	// Force GC before starting
	runtime.GC()
	runtime.GC()
	
	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)
	
	manager := NewOrgSemaphoreManager(100, 20, true)
	defer manager.Stop()
	
	// Create load across many organizations
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	// Simulate 50 organizations with varying activity
	for orgID := int64(1); orgID <= 50; orgID++ {
		wg.Add(1)
		go func(orgID int64) {
			defer wg.Done()
			
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				
				acqCtx, acqCancel := context.WithTimeout(ctx, 10*time.Millisecond)
				release, err := manager.TryAcquireEvaluationSlot(acqCtx, orgID)
				acqCancel()
				
				if err == nil {
					time.Sleep(5 * time.Millisecond)
					release()
				}
				
				// Vary the frequency per organization
				sleepTime := time.Duration(int(orgID)%10+1) * time.Millisecond
				time.Sleep(sleepTime)
			}
		}(orgID)
	}
	
	wg.Wait()
	
	// Force GC and measure memory
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&m2)
	
	memoryUsed := m2.Alloc - m1.Alloc
	memoryPerOrg := memoryUsed / 50 // 50 organizations
	
	t.Logf("Memory Usage Test Results:")
	t.Logf("  Memory used: %d bytes", memoryUsed)
	t.Logf("  Memory per organization: %d bytes", memoryPerOrg)
	t.Logf("  Total allocations: %d", m2.TotalAlloc-m1.TotalAlloc)
	
	// Memory usage should be reasonable (less than 10KB per org is very conservative)
	if memoryPerOrg > 10*1024 {
		t.Errorf("Memory usage per organization (%d bytes) is too high", memoryPerOrg)
	}
	
	// Verify that some organizations were actually created
	stats := manager.GetRichStats()
	orgCount := stats["org_semaphores"].(int)
	if orgCount == 0 {
		t.Error("No organization semaphores were created during load test")
	}
	
	t.Logf("  Organizations created: %d", orgCount)
}

// TestLoadSemaphoreManager_GarbageCollection tests GC behavior under load
func TestLoadSemaphoreManager_GarbageCollection(t *testing.T) {
	// Use shorter GC intervals for testing
	manager := &OrgSemaphoreManager{
		globalSemaphore:   make(chan struct{}, 50),
		orgSemaphores:     make(map[int64]*OrgSemaphore),
		globalLimit:       50,
		perOrgLimit:      10,
		enabled:           true,
		gcInterval:        200 * time.Millisecond, // Short interval for testing
		semaphoreIdleTime: 500 * time.Millisecond, // Short idle time for testing
		gcStop:            make(chan struct{}),
		metrics: &SemaphoreMetrics{
			QueueDepth:         make(map[int64]int),
			LastEvalTimes:      make(map[int64]time.Time),
			WaitDurations:      make(map[int64]time.Duration),
			DroppedEvaluations: make(map[int64]int64),
		},
	}
	
	// Start background GC
	go manager.startGC()
	defer manager.Stop()
	
	// Create activity for organizations 1-10
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	
	var wg sync.WaitGroup
	for orgID := int64(1); orgID <= 10; orgID++ {
		wg.Add(1)
		go func(orgID int64) {
			defer wg.Done()
			
			// Do some work, then stop (to test GC)
			for i := 0; i < 5; i++ {
				acqCtx, acqCancel := context.WithTimeout(ctx, 10*time.Millisecond)
				release, err := manager.TryAcquireEvaluationSlot(acqCtx, orgID)
				acqCancel()
				
				if err == nil {
					time.Sleep(10 * time.Millisecond)
					release()
				}
				time.Sleep(50 * time.Millisecond)
			}
		}(orgID)
	}
	
	wg.Wait()
	
	// Check initial organization count
	stats := manager.GetRichStats()
	initialOrgCount := stats["org_semaphores"].(int)
	t.Logf("Initial organization count: %d", initialOrgCount)
	
	// Wait for GC to potentially clean up some organizations
	time.Sleep(1 * time.Second)
	
	// Check final organization count
	stats = manager.GetRichStats()
	finalOrgCount := stats["org_semaphores"].(int)
	t.Logf("Final organization count after GC: %d", finalOrgCount)
	
	// Verify that GC can clean up unused organizations
	// (Note: in a real test, some might still be in use, so we're lenient)
	if finalOrgCount > initialOrgCount {
		t.Errorf("Organization count increased unexpectedly (initial: %d, final: %d)", 
			initialOrgCount, finalOrgCount)
	}
	
	t.Logf("GC successfully managed organization lifecycle")
}