package semaphore

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// OrgSemaphoreManager manages per-organization concurrency control for alert evaluations
type OrgSemaphoreManager struct {
	// Global semaphore for all evaluations
	globalSemaphore chan struct{}
	
	// Per-organization semaphores with metadata
	orgSemaphores map[int64]*OrgSemaphore
	
	// Mutex for thread-safe access to orgSemaphores
	mu sync.RWMutex
	
	// Configuration
	globalLimit int
	perOrgLimit int
	enabled     bool
	
	// GC settings
	gcInterval         time.Duration
	semaphoreIdleTime  time.Duration
	gcStop             chan struct{}
	
	// Metrics
	metrics *SemaphoreMetrics
}

// OrgSemaphore represents a per-organization semaphore with metadata
type OrgSemaphore struct {
	semaphore       chan struct{}
	lastAccessed    int64 // Use atomic operations for time
	mu              sync.RWMutex
	
	// Instrumentation - use atomic operations for counters
	lastEvaluation  int64 // Use atomic operations for time
	totalDropped    int64 // Use atomic operations
	totalAcquired   int64 // Use atomic operations
}

// SemaphoreMetrics tracks metrics for monitoring and alerting
type SemaphoreMetrics struct {
	QueueDepth         map[int64]int
	LastEvalTimes      map[int64]time.Time
	WaitDurations      map[int64]time.Duration
	DroppedEvaluations map[int64]int64
}

// NewOrgSemaphoreManager creates a new per-organization semaphore manager
func NewOrgSemaphoreManager(globalLimit, perOrgLimit int, enabled bool) *OrgSemaphoreManager {
	osm := &OrgSemaphoreManager{
		globalSemaphore:   make(chan struct{}, globalLimit),
		orgSemaphores:     make(map[int64]*OrgSemaphore),
		mu:                sync.RWMutex{},
		globalLimit:       globalLimit,
		perOrgLimit:       perOrgLimit,
		enabled:           enabled,
		gcInterval:        5 * time.Minute,
		semaphoreIdleTime: 15 * time.Minute,
		gcStop:            make(chan struct{}),
		metrics:           &SemaphoreMetrics{
			QueueDepth:         make(map[int64]int),
			LastEvalTimes:      make(map[int64]time.Time),
			WaitDurations:      make(map[int64]time.Duration),
			DroppedEvaluations: make(map[int64]int64),
		},
	}
	
	// Start background GC
	go osm.startGC()
	
	return osm
}

// getOrCreateOrgSemaphore gets or creates a semaphore for the given organization
func (osm *OrgSemaphoreManager) getOrCreateOrgSemaphore(orgID int64) *OrgSemaphore {
	osm.mu.RLock()
	if sem, exists := osm.orgSemaphores[orgID]; exists {
		atomic.StoreInt64(&sem.lastAccessed, time.Now().Unix())
		osm.mu.RUnlock()
		return sem
	}
	osm.mu.RUnlock()
	
	osm.mu.Lock()
	defer osm.mu.Unlock()
	
	// Double-check after acquiring write lock
	if sem, exists := osm.orgSemaphores[orgID]; exists {
		return sem
	}
	
	sem := &OrgSemaphore{
		semaphore:    make(chan struct{}, osm.perOrgLimit),
		lastAccessed: time.Now().Unix(),
		mu:           sync.RWMutex{},
	}
	osm.orgSemaphores[orgID] = sem
	return sem
}

// TryAcquireEvaluationSlot attempts to acquire both global and per-org semaphore slots
func (osm *OrgSemaphoreManager) TryAcquireEvaluationSlot(ctx context.Context, orgID int64) (func(), error) {
	if !osm.enabled {
		return func() {}, nil
	}
	
	startTime := time.Now()
	
	// Acquire global semaphore first with context
	select {
	case osm.globalSemaphore <- struct{}{}:
		// Global slot acquired
	case <-ctx.Done():
		osm.recordDroppedEvaluation(orgID, time.Since(startTime))
		return nil, ctx.Err()
	default:
		osm.recordDroppedEvaluation(orgID, time.Since(startTime))
		return nil, ErrGlobalConcurrencyLimitReached
	}
	
	// Acquire per-org semaphore with context
	orgSem := osm.getOrCreateOrgSemaphore(orgID)
	select {
	case orgSem.semaphore <- struct{}{}:
		// Org slot acquired - update metrics
		osm.recordSuccessfulAcquisition(orgID, time.Since(startTime))
	case <-ctx.Done():
		// Release global semaphore if org semaphore times out
		<-osm.globalSemaphore
		osm.recordDroppedEvaluation(orgID, time.Since(startTime))
		return nil, ctx.Err()
	default:
		// Release global semaphore if org semaphore is full
		<-osm.globalSemaphore
		osm.recordDroppedEvaluation(orgID, time.Since(startTime))
		return nil, ErrOrgConcurrencyLimitReached
	}
	
	// Return release function
	return func() {
		<-orgSem.semaphore
		<-osm.globalSemaphore
		osm.recordEvaluationComplete(orgID)
	}, nil
}

// AcquireEvaluationSlot is a legacy method for backward compatibility
func (osm *OrgSemaphoreManager) AcquireEvaluationSlot(orgID int64) (func(), error) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	return osm.TryAcquireEvaluationSlot(ctx, orgID)
}

// recordDroppedEvaluation updates metrics when an evaluation is dropped
func (osm *OrgSemaphoreManager) recordDroppedEvaluation(orgID int64, waitDuration time.Duration) {
	// Use read lock first to find the org semaphore
	osm.mu.RLock()
	orgSem, exists := osm.orgSemaphores[orgID]
	osm.mu.RUnlock()
	
	// Update org-specific metrics using atomic operations
	if exists {
		atomic.AddInt64(&orgSem.totalDropped, 1)
		atomic.StoreInt64(&orgSem.lastAccessed, time.Now().Unix())
	}
	
	// Update manager metrics separately
	osm.mu.Lock()
	osm.metrics.DroppedEvaluations[orgID]++
	osm.metrics.WaitDurations[orgID] = waitDuration
	if exists {
		osm.metrics.QueueDepth[orgID] = len(orgSem.semaphore)
	}
	osm.mu.Unlock()
}

// recordSuccessfulAcquisition updates metrics when a semaphore is successfully acquired
func (osm *OrgSemaphoreManager) recordSuccessfulAcquisition(orgID int64, waitDuration time.Duration) {
	// Use read lock first to find the org semaphore
	osm.mu.RLock()
	orgSem, exists := osm.orgSemaphores[orgID]
	osm.mu.RUnlock()
	
	// Update org-specific metrics using atomic operations
	if exists {
		atomic.AddInt64(&orgSem.totalAcquired, 1)
		atomic.StoreInt64(&orgSem.lastAccessed, time.Now().Unix())
	}
	
	// Update manager metrics separately
	osm.mu.Lock()
	osm.metrics.WaitDurations[orgID] = waitDuration
	if exists {
		osm.metrics.QueueDepth[orgID] = len(orgSem.semaphore)
	}
	osm.mu.Unlock()
}

// recordEvaluationComplete updates metrics when an evaluation completes
func (osm *OrgSemaphoreManager) recordEvaluationComplete(orgID int64) {
	now := time.Now()
	
	// Use read lock first to find the org semaphore
	osm.mu.RLock()
	orgSem, exists := osm.orgSemaphores[orgID]
	osm.mu.RUnlock()
	
	// Update org-specific metrics using atomic operations
	if exists {
		atomic.StoreInt64(&orgSem.lastEvaluation, now.Unix())
		atomic.StoreInt64(&orgSem.lastAccessed, now.Unix())
	}
	
	// Update manager metrics separately
	osm.mu.Lock()
	osm.metrics.LastEvalTimes[orgID] = now
	osm.mu.Unlock()
}

// startGC runs background garbage collection for unused semaphores
func (osm *OrgSemaphoreManager) startGC() {
	// Read the GC interval once at startup to avoid race condition
	osm.mu.RLock()
	gcInterval := osm.gcInterval
	osm.mu.RUnlock()
	
	ticker := time.NewTicker(gcInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			osm.gcUnusedSemaphores()
		case <-osm.gcStop:
			return
		}
	}
}

// gcUnusedSemaphores removes unused semaphores to prevent memory leaks
func (osm *OrgSemaphoreManager) gcUnusedSemaphores() {
	osm.mu.Lock()
	defer osm.mu.Unlock()
	
	now := time.Now()
	for orgID, sem := range osm.orgSemaphores {
		lastAccessed := time.Unix(atomic.LoadInt64(&sem.lastAccessed), 0)
		idle := now.Sub(lastAccessed) > osm.semaphoreIdleTime
		empty := len(sem.semaphore) == 0
		
		if idle && empty {
			delete(osm.orgSemaphores, orgID)
			delete(osm.metrics.QueueDepth, orgID)
			delete(osm.metrics.LastEvalTimes, orgID)
			delete(osm.metrics.WaitDurations, orgID)
			delete(osm.metrics.DroppedEvaluations, orgID)
		}
	}
}

// UpdateLimits supports runtime reconfiguration of semaphore limits
func (osm *OrgSemaphoreManager) UpdateLimits(globalLimit, perOrgLimit int) {
	osm.mu.Lock()
	defer osm.mu.Unlock()
	
	// Update global semaphore
	if globalLimit != osm.globalLimit {
		newGlobalSem := make(chan struct{}, globalLimit)
		// Transfer existing tokens up to new limit
		currentTokens := len(osm.globalSemaphore)
		tokensToTransfer := currentTokens
		if tokensToTransfer > globalLimit {
			tokensToTransfer = globalLimit
		}
		
		for i := 0; i < tokensToTransfer; i++ {
			newGlobalSem <- struct{}{}
		}
		osm.globalSemaphore = newGlobalSem
		osm.globalLimit = globalLimit
	}
	
	// Update per-org semaphores
	if perOrgLimit != osm.perOrgLimit {
		for _, orgSem := range osm.orgSemaphores {
			orgSem.mu.Lock()
			newSem := make(chan struct{}, perOrgLimit)
			
			// Transfer existing tokens up to new limit
			currentTokens := len(orgSem.semaphore)
			tokensToTransfer := currentTokens
			if tokensToTransfer > perOrgLimit {
				tokensToTransfer = perOrgLimit
			}
			
			for i := 0; i < tokensToTransfer; i++ {
				newSem <- struct{}{}
			}
			orgSem.semaphore = newSem
			orgSem.mu.Unlock()
		}
		osm.perOrgLimit = perOrgLimit
	}
}

// GetRichStats returns comprehensive statistics for monitoring
func (osm *OrgSemaphoreManager) GetRichStats() map[string]interface{} {
	osm.mu.RLock()
	defer osm.mu.RUnlock()
	
	stats := map[string]interface{}{
		"global_limit":     osm.globalLimit,
		"global_active":    len(osm.globalSemaphore),
		"per_org_limit":    osm.perOrgLimit,
		"enabled":          osm.enabled,
		"org_semaphores":   len(osm.orgSemaphores),
	}
	
	orgStats := make(map[int64]map[string]interface{})
	for orgID, sem := range osm.orgSemaphores {
		orgStats[orgID] = map[string]interface{}{
			"active":          len(sem.semaphore),
			"queue_depth":     osm.metrics.QueueDepth[orgID],
			"last_eval":       osm.metrics.LastEvalTimes[orgID],
			"wait_duration":   osm.metrics.WaitDurations[orgID],
			"total_dropped":   atomic.LoadInt64(&sem.totalDropped),
			"total_acquired":  atomic.LoadInt64(&sem.totalAcquired),
			"last_accessed":   time.Unix(atomic.LoadInt64(&sem.lastAccessed), 0),
		}
	}
	stats["org_details"] = orgStats
	
	return stats
}

// Stop gracefully shuts down the semaphore manager
func (osm *OrgSemaphoreManager) Stop() {
	close(osm.gcStop)
}

// Error definitions
var (
	ErrGlobalConcurrencyLimitReached = errors.New("global evaluation concurrency limit reached")
	ErrOrgConcurrencyLimitReached    = errors.New("organization evaluation concurrency limit reached")
)