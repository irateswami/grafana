package schedule

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grafana/grafana/pkg/services/ngalert/semaphore"
)

// TestLoadScheduler_SemaphoreIntegration tests the scheduler's semaphore integration under load
func TestLoadScheduler_SemaphoreIntegration(t *testing.T) {
	tests := []struct {
		name                string
		globalLimit         int
		perOrgLimit        int
		organizations      int
		workersPerOrg      int
		duration           time.Duration
		evaluationTimeout  time.Duration
		expectedMinSuccess float64
	}{
		{
			name:              "Production_Load",
			globalLimit:       50,
			perOrgLimit:      10,
			organizations:    5,
			workersPerOrg:    15,
			duration:         10 * time.Second,
			evaluationTimeout: 30 * time.Second,
			expectedMinSuccess: 0.05,
		},
		{
			name:              "High_Contention",
			globalLimit:       20,
			perOrgLimit:      5,
			organizations:    8,
			workersPerOrg:    20,
			duration:         5 * time.Second,
			evaluationTimeout: 15 * time.Second,
			expectedMinSuccess: 0.03,
		},
		{
			name:              "Large_Scale",
			globalLimit:       100,
			perOrgLimit:      20,
			organizations:    10,
			workersPerOrg:    25,
			duration:         15 * time.Second,
			evaluationTimeout: 45 * time.Second,
			expectedMinSuccess: 0.15,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create semaphore manager
			manager := semaphore.NewOrgSemaphoreManager(tt.globalLimit, tt.perOrgLimit, true)
			defer manager.Stop()
			
			var (
				totalAttempts      int64
				totalSuccesses     int64
				totalFailures      int64
				totalTimeouts      int64
				totalEvaluationTime int64
			)
			
			ctx, cancel := context.WithTimeout(context.Background(), tt.duration)
			defer cancel()
			
			var wg sync.WaitGroup
			
			// Start workers for each organization
			for orgID := int64(1); orgID <= int64(tt.organizations); orgID++ {
				for worker := 0; worker < tt.workersPerOrg; worker++ {
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
							
							// Simulate scheduler's semaphore acquisition process
							evalCtx, evalCancel := context.WithTimeout(ctx, tt.evaluationTimeout)
							
							release, err := manager.TryAcquireEvaluationSlot(evalCtx, orgID)
							evalCancel()
							
							if err != nil {
								if err == context.DeadlineExceeded {
									atomic.AddInt64(&totalTimeouts, 1)
								} else {
									atomic.AddInt64(&totalFailures, 1)
								}
								continue
							}
							
							atomic.AddInt64(&totalSuccesses, 1)
							
							// Simulate alert evaluation work
							evaluationStart := time.Now()
							
							// Variable evaluation time based on organization and worker
							evalDuration := time.Duration(50+orgID*2+int64(workerID)*3) * time.Millisecond
							time.Sleep(evalDuration)
							
							atomic.AddInt64(&totalEvaluationTime, int64(time.Since(evaluationStart)))
							
							release()
							
							// Brief pause between evaluations
							time.Sleep(time.Duration(10+workerID*2) * time.Millisecond)
						}
					}(orgID, worker)
				}
			}
			
			wg.Wait()
			
			// Calculate metrics
			attempts := atomic.LoadInt64(&totalAttempts)
			successes := atomic.LoadInt64(&totalSuccesses)
			failures := atomic.LoadInt64(&totalFailures)
			timeouts := atomic.LoadInt64(&totalTimeouts)
			avgEvalTime := time.Duration(atomic.LoadInt64(&totalEvaluationTime) / successes)
			
			successRate := float64(successes) / float64(attempts) * 100
			
			t.Logf("Scheduler Load Test Results for %s:", tt.name)
			t.Logf("  Duration: %v", tt.duration)
			t.Logf("  Organizations: %d", tt.organizations)
			t.Logf("  Workers per org: %d", tt.workersPerOrg)
			t.Logf("  Global limit: %d", tt.globalLimit)
			t.Logf("  Per-org limit: %d", tt.perOrgLimit)
			t.Logf("  Evaluation timeout: %v", tt.evaluationTimeout)
			t.Logf("  Total attempts: %d", attempts)
			t.Logf("  Successful evaluations: %d", successes)
			t.Logf("  Concurrency failures: %d", failures)
			t.Logf("  Timeout failures: %d", timeouts)
			t.Logf("  Success rate: %.2f%%", successRate)
			t.Logf("  Average evaluation time: %v", avgEvalTime)
			
			// Verify minimum success rate
			if successRate < tt.expectedMinSuccess {
				t.Errorf("Success rate %.2f%% is below expected minimum %.2f%%", 
					successRate, tt.expectedMinSuccess)
			}
			
			// Verify meaningful load
			if attempts < 50 {
				t.Errorf("Not enough attempts (%d) to validate load test", attempts)
			}
			
			// Get semaphore statistics
			stats := manager.GetRichStats()
			t.Logf("Final semaphore state:")
			t.Logf("  Global active: %v", stats["global_active"])
			t.Logf("  Organizations tracked: %v", stats["org_semaphores"])
		})
	}
}

// TestLoadScheduler_BackpressureHandling tests how the system handles backpressure
func TestLoadScheduler_BackpressureHandling(t *testing.T) {
	// Create a system with very low limits to force backpressure
	manager := semaphore.NewOrgSemaphoreManager(10, 3, true)
	defer manager.Stop()
	
	var (
		totalAttempts   int64
		timeoutErrors   int64
		limitErrors     int64
		successes       int64
		backpressureDetected int64
	)
	
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	
	var wg sync.WaitGroup
	
	// Create intense load that will definitely cause backpressure
	for orgID := int64(1); orgID <= 5; orgID++ {
		for worker := 0; worker < 20; worker++ { // 100 total workers, 10 limit = guaranteed contention
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
					
					// Use very short timeout to trigger backpressure detection
					evalCtx, evalCancel := context.WithTimeout(ctx, 5*time.Millisecond)
					
					release, err := manager.TryAcquireEvaluationSlot(evalCtx, orgID)
					evalCancel()
					
					if err != nil {
						if err == context.DeadlineExceeded {
							atomic.AddInt64(&timeoutErrors, 1)
							atomic.AddInt64(&backpressureDetected, 1)
						} else {
							atomic.AddInt64(&limitErrors, 1)
						}
						continue
					}
					
					atomic.AddInt64(&successes, 1)
					
					// Simulate longer evaluation to increase contention
					time.Sleep(20 * time.Millisecond)
					release()
					
					// Very brief pause
					time.Sleep(1 * time.Millisecond)
				}
			}(orgID, worker)
		}
	}
	
	wg.Wait()
	
	attempts := atomic.LoadInt64(&totalAttempts)
	timeouts := atomic.LoadInt64(&timeoutErrors)
	limits := atomic.LoadInt64(&limitErrors)
	success := atomic.LoadInt64(&successes)
	backpressure := atomic.LoadInt64(&backpressureDetected)
	
	t.Logf("Backpressure Handling Test Results:")
	t.Logf("  Total attempts: %d", attempts)
	t.Logf("  Successful acquisitions: %d", success)
	t.Logf("  Timeout errors (backpressure): %d", timeouts)
	t.Logf("  Limit errors: %d", limits)
	t.Logf("  Backpressure events: %d", backpressure)
	t.Logf("  Success rate: %.2f%%", float64(success)/float64(attempts)*100)
	t.Logf("  Backpressure rate: %.2f%%", float64(backpressure)/float64(attempts)*100)
	
	// Verify that the system correctly handles load by rejecting excess requests
	if limitErrors == 0 {
		t.Error("No limit errors were detected despite intense load")
	}
	
	// Verify some evaluations still succeeded despite load
	if success == 0 {
		t.Error("No evaluations succeeded - system may be completely blocked")
	}
	
	// Verify reasonable distribution of errors
	limitErrorRate := float64(limitErrors) / float64(attempts) * 100
	if limitErrorRate < 90 { // Should see significant limit errors with these limits
		t.Errorf("Limit error rate %.2f%% is lower than expected for this load", limitErrorRate)
	}
}

// TestLoadScheduler_OrganizationIsolation tests that organizations don't affect each other
func TestLoadScheduler_OrganizationIsolation(t *testing.T) {
	manager := semaphore.NewOrgSemaphoreManager(50, 8, true)
	defer manager.Stop()
	
	// Track successes per organization
	orgSuccesses := make([]int64, 6) // orgs 1-5
	var totalAttempts int64
	
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	
	var wg sync.WaitGroup
	
	// Create different load patterns for each organization
	loadPatterns := []struct {
		workers     int
		workDuration time.Duration
		pauseDuration time.Duration
	}{
		{5, 10*time.Millisecond, 20*time.Millisecond},  // Light load
		{10, 15*time.Millisecond, 15*time.Millisecond}, // Medium load  
		{15, 20*time.Millisecond, 10*time.Millisecond}, // Heavy load
		{8, 5*time.Millisecond, 25*time.Millisecond},   // Bursty load
		{12, 25*time.Millisecond, 5*time.Millisecond},  // Long-running load
	}
	
	for orgID := int64(1); orgID <= 5; orgID++ {
		pattern := loadPatterns[orgID-1]
		
		for worker := 0; worker < pattern.workers; worker++ {
			wg.Add(1)
			go func(orgID int64, pattern struct{workers int; workDuration, pauseDuration time.Duration}) {
				defer wg.Done()
				
				for {
					select {
					case <-ctx.Done():
						return
					default:
					}
					
					atomic.AddInt64(&totalAttempts, 1)
					
					evalCtx, evalCancel := context.WithTimeout(ctx, 50*time.Millisecond)
					release, err := manager.TryAcquireEvaluationSlot(evalCtx, orgID)
					evalCancel()
					
					if err == nil {
						atomic.AddInt64(&orgSuccesses[orgID], 1)
						time.Sleep(pattern.workDuration)
						release()
					}
					
					time.Sleep(pattern.pauseDuration)
				}
			}(orgID, pattern)
		}
	}
	
	wg.Wait()
	
	attempts := atomic.LoadInt64(&totalAttempts)
	totalSuccesses := int64(0)
	for i := int64(1); i <= 5; i++ {
		totalSuccesses += atomic.LoadInt64(&orgSuccesses[i])
	}
	
	t.Logf("Organization Isolation Test Results:")
	t.Logf("  Total attempts: %d", attempts)
	t.Logf("  Total successes: %d", totalSuccesses)
	t.Logf("  Overall success rate: %.2f%%", float64(totalSuccesses)/float64(attempts)*100)
	
	// Log per-organization results
	for orgID := int64(1); orgID <= 5; orgID++ {
		successes := atomic.LoadInt64(&orgSuccesses[orgID])
		pattern := loadPatterns[orgID-1]
		t.Logf("  Org %d (%d workers, %v work, %v pause): %d successes", 
			orgID, pattern.workers, pattern.workDuration, pattern.pauseDuration, successes)
	}
	
	// Verify that all organizations got some successful evaluations
	for orgID := int64(1); orgID <= 5; orgID++ {
		successes := atomic.LoadInt64(&orgSuccesses[orgID])
		if successes == 0 {
			t.Errorf("Organization %d was completely starved", orgID)
		}
	}
	
	// Verify reasonable distribution (no single org should dominate completely)
	maxSuccesses := int64(0)
	for orgID := int64(1); orgID <= 5; orgID++ {
		successes := atomic.LoadInt64(&orgSuccesses[orgID])
		if successes > maxSuccesses {
			maxSuccesses = successes
		}
	}
	
	dominancePercentage := float64(maxSuccesses) / float64(totalSuccesses) * 100
	if dominancePercentage > 70 { // No single org should have more than 70%
		t.Errorf("One organization dominated too much (%.1f%% of all successes)", dominancePercentage)
	}
	
	t.Logf("  Max organization dominance: %.1f%%", dominancePercentage)
}

// BenchmarkScheduler_SemaphoreAcquisition benchmarks the scheduler's semaphore operations
func BenchmarkScheduler_SemaphoreAcquisition(b *testing.B) {
	scenarios := []struct {
		name        string
		globalLimit int
		perOrgLimit int
		orgs        int
		enabled     bool
	}{
		{"Small_Enabled", 20, 5, 3, true},
		{"Medium_Enabled", 50, 10, 5, true},
		{"Large_Enabled", 100, 20, 10, true},
		{"Small_Disabled", 20, 5, 3, false},
		{"Medium_Disabled", 50, 10, 5, false},
		{"Large_Disabled", 100, 20, 10, false},
	}
	
	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			manager := semaphore.NewOrgSemaphoreManager(scenario.globalLimit, scenario.perOrgLimit, scenario.enabled)
			defer manager.Stop()
			
			b.ResetTimer()
			b.ReportAllocs()
			
			b.RunParallel(func(pb *testing.PB) {
				orgID := int64(1 + (b.N % scenario.orgs))
				ctx := context.Background()
				
				for pb.Next() {
					// Simulate the scheduler's evaluation process
					evalCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
					
					release, err := manager.TryAcquireEvaluationSlot(evalCtx, orgID)
					if err == nil && release != nil {
						// Simulate brief evaluation work
						time.Sleep(100 * time.Microsecond)
						release()
					}
					
					cancel()
				}
			})
		})
	}
}

// TestLoadScheduler_ConfigurationVariations tests different configuration scenarios
func TestLoadScheduler_ConfigurationVariations(t *testing.T) {
	configurations := []struct {
		name        string
		globalLimit int
		perOrgLimit int
		description string
	}{
		{"Conservative", 30, 5, "Conservative limits for stability"},
		{"Balanced", 50, 10, "Balanced limits for typical deployments"},
		{"Aggressive", 100, 25, "Aggressive limits for high-performance deployments"},
		{"GlobalBottleneck", 20, 15, "Global limit is the bottleneck"},
		{"OrgBottleneck", 100, 5, "Per-org limit is the bottleneck"},
	}
	
	for _, config := range configurations {
		t.Run(config.name, func(t *testing.T) {
			manager := semaphore.NewOrgSemaphoreManager(config.globalLimit, config.perOrgLimit, true)
			defer manager.Stop()
			
			var totalAttempts, totalSuccesses int64
			
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			
			var wg sync.WaitGroup
			
			// Standard load pattern across 5 organizations
			for orgID := int64(1); orgID <= 5; orgID++ {
				for worker := 0; worker < 10; worker++ {
					wg.Add(1)
					go func(orgID int64) {
						defer wg.Done()
						
						for {
							select {
							case <-ctx.Done():
								return
							default:
							}
							
							atomic.AddInt64(&totalAttempts, 1)
							
							evalCtx, evalCancel := context.WithTimeout(ctx, 25*time.Millisecond)
							release, err := manager.TryAcquireEvaluationSlot(evalCtx, orgID)
							evalCancel()
							
							if err == nil {
								atomic.AddInt64(&totalSuccesses, 1)
								time.Sleep(10 * time.Millisecond) // Standard work duration
								release()
							}
							
							time.Sleep(5 * time.Millisecond) // Standard pause
						}
					}(orgID)
				}
			}
			
			wg.Wait()
			
			attempts := atomic.LoadInt64(&totalAttempts)
			successes := atomic.LoadInt64(&totalSuccesses)
			successRate := float64(successes) / float64(attempts) * 100
			
			stats := manager.GetRichStats()
			
			t.Logf("Configuration Test: %s (%s)", config.name, config.description)
			t.Logf("  Global limit: %d, Per-org limit: %d", config.globalLimit, config.perOrgLimit)
			t.Logf("  Total attempts: %d", attempts)
			t.Logf("  Total successes: %d", successes)
			t.Logf("  Success rate: %.2f%%", successRate)
			t.Logf("  Organizations created: %v", stats["org_semaphores"])
			
			// Log expected bottleneck analysis
			globalUtilization := float64(stats["global_active"].(int)) / float64(config.globalLimit) * 100
			t.Logf("  Global utilization at end: %.1f%%", globalUtilization)
			
			// Verify reasonable performance for each configuration
			minExpectedSuccess := 15.0 // Very conservative minimum
			if successRate < minExpectedSuccess {
				t.Errorf("Success rate %.2f%% is below minimum threshold %.2f%% for configuration %s", 
					successRate, minExpectedSuccess, config.name)
			}
		})
	}
}