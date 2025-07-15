package schedule

import (
	"context"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/benbjohnson/clock"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/grafana-plugin-sdk-go/data"

	"github.com/grafana/grafana/pkg/infra/log"
	"github.com/grafana/grafana/pkg/infra/tracing"
	"github.com/grafana/grafana/pkg/services/ngalert/api/tooling/definitions"
	"github.com/grafana/grafana/pkg/services/ngalert/eval"
	"github.com/grafana/grafana/pkg/services/ngalert/metrics"
	ngmodels "github.com/grafana/grafana/pkg/services/ngalert/models"
	"github.com/grafana/grafana/pkg/services/ngalert/semaphore"
	"github.com/grafana/grafana/pkg/services/ngalert/state"
	"github.com/grafana/grafana/pkg/setting"
	"github.com/grafana/grafana/pkg/util/ticker"
)

// ScheduleService is an interface for a service that schedules the evaluation
// of alert rules.
type ScheduleService interface {
	// Run the scheduler until the context is canceled or the scheduler returns
	// an error. The scheduler is terminated when this function returns.
	Run(context.Context) error
}

// retryDelay represents how long to wait between each failed rule evaluation.
const retryDelay = 1 * time.Second

// AlertsSender is an interface for a service that is responsible for sending notifications to the end-user.
//
//go:generate mockery --name AlertsSender --structname AlertsSenderMock --inpackage --filename alerts_sender_mock.go --with-expecter
type AlertsSender interface {
	Send(ctx context.Context, key ngmodels.AlertRuleKey, alerts definitions.PostableAlerts)
}

// RulesStore is a store that provides alert rules for scheduling
type RulesStore interface {
	GetAlertRulesKeysForScheduling(ctx context.Context) ([]ngmodels.AlertRuleKeyWithVersion, error)
	GetAlertRulesForScheduling(ctx context.Context, query *ngmodels.GetAlertRulesForSchedulingQuery) error
}

type RecordingWriter interface {
	WriteDatasource(ctx context.Context, dsUID string, name string, t time.Time, frames data.Frames, orgID int64, extraLabels map[string]string) error
}

// AlertRuleStopReasonProvider is an interface for determining the reason why an alert rule was stopped.
type AlertRuleStopReasonProvider interface {
	// FindReason returns two values:
	// 1. The first value is the reason for stopping the alert rule (error type).
	// 2. The second value is an error indicating any issues that occurred while determining the stop reason.
	//	  If this is non-nil, the scheduler uses the default reason.
	FindReason(ctx context.Context, logger log.Logger, key ngmodels.AlertRuleKeyWithGroup) (error, error)
}

type schedule struct {
	// base tick rate (fastest possible configured check)
	baseInterval time.Duration

	// each rule gets its own channel and routine
	registry ruleRegistry

	maxAttempts int64

	clock clock.Clock

	// evalApplied is only used for tests: test code can set it to non-nil
	// function, and then it'll be called from the event loop whenever the
	// message from evalApplied is handled.
	evalAppliedFunc func(ngmodels.AlertRuleKey, time.Time)

	// stopApplied is only used for tests: test code can set it to non-nil
	// function, and then it'll be called from the event loop whenever the
	// message from stopApplied is handled.
	stopAppliedFunc func(ngmodels.AlertRuleKey)

	ruleStopReasonProvider AlertRuleStopReasonProvider

	log log.Logger

	evaluatorFactory eval.EvaluatorFactory

	ruleStore RulesStore

	stateManager *state.Manager

	appURL               *url.URL
	disableGrafanaFolder bool
	jitterEvaluations    JitterStrategy
	rrCfg                setting.RecordingRuleSettings

	metrics *metrics.Scheduler

	alertsSender    AlertsSender
	minRuleInterval time.Duration

	// schedulableAlertRules contains the alert rules that are considered for
	// evaluation in the current tick. The evaluation of an alert rule in the
	// current tick depends on its evaluation interval and when it was
	// last evaluated.
	schedulableAlertRules alertRulesRegistry

	tracer tracing.Tracer

	recordingWriter RecordingWriter

	// Per-organization semaphore manager
	orgSemaphoreManager *semaphore.OrgSemaphoreManager
	
	// Evaluation timeout for context cancellation
	evaluationTimeout time.Duration

	// Phase 3: Starvation detection
	starvationDetector *StarvationDetector

	// Phase 3: Remediation manager for enhanced error messages
	remediationManager *RemediationManager
}

// SchedulerCfg is the scheduler configuration.
type SchedulerCfg struct {
	MaxAttempts            int64
	BaseInterval           time.Duration
	C                      clock.Clock
	MinRuleInterval        time.Duration
	DisableGrafanaFolder   bool
	RecordingRulesCfg      setting.RecordingRuleSettings
	AppURL                 *url.URL
	JitterEvaluations      JitterStrategy
	EvaluatorFactory       eval.EvaluatorFactory
	RuleStore              RulesStore
	Metrics                *metrics.Scheduler
	AlertSender            AlertsSender
	Tracer                 tracing.Tracer
	Log                    log.Logger
	RecordingWriter        RecordingWriter
	RuleStopReasonProvider AlertRuleStopReasonProvider

	// Semaphore configuration for concurrency control
	MaxEvaluationConcurrency       int
	MaxEvaluationConcurrencyPerOrg int
	EnablePerOrgEvaluationLimits   bool
	EvaluationTimeout              time.Duration

	// Phase 3: Starvation detection configuration
	StarvationThreshold        time.Duration
	StarvationCheckInterval    time.Duration
	EnableStarvationDetection  bool
}

// NewScheduler returns a new scheduler.
func NewScheduler(cfg SchedulerCfg, stateManager *state.Manager) *schedule {
	const minMaxAttempts = int64(1)
	if cfg.MaxAttempts < minMaxAttempts {
		cfg.Log.Warn("Invalid scheduler maxAttempts, using a safe minimum", "configured", cfg.MaxAttempts, "actual", minMaxAttempts)
		cfg.MaxAttempts = minMaxAttempts
	}

	sch := schedule{
		registry:               newRuleRegistry(),
		maxAttempts:            cfg.MaxAttempts,
		clock:                  cfg.C,
		baseInterval:           cfg.BaseInterval,
		log:                    cfg.Log,
		evaluatorFactory:       cfg.EvaluatorFactory,
		ruleStore:              cfg.RuleStore,
		metrics:                cfg.Metrics,
		appURL:                 cfg.AppURL,
		disableGrafanaFolder:   cfg.DisableGrafanaFolder,
		jitterEvaluations:      cfg.JitterEvaluations,
		rrCfg:                  cfg.RecordingRulesCfg,
		stateManager:           stateManager,
		minRuleInterval:        cfg.MinRuleInterval,
		schedulableAlertRules:  alertRulesRegistry{rules: make(map[ngmodels.AlertRuleKey]*ngmodels.AlertRule)},
		alertsSender:           cfg.AlertSender,
		tracer:                 cfg.Tracer,
		recordingWriter:        cfg.RecordingWriter,
		ruleStopReasonProvider: cfg.RuleStopReasonProvider,
		orgSemaphoreManager:    semaphore.NewOrgSemaphoreManager(
			cfg.MaxEvaluationConcurrency,
			cfg.MaxEvaluationConcurrencyPerOrg,
			cfg.EnablePerOrgEvaluationLimits,
		),
		evaluationTimeout:      cfg.EvaluationTimeout,
	}

	// Initialize starvation detector if enabled
	if cfg.EnableStarvationDetection {
		starvationCallback := func(orgID int64, duration time.Duration) {
			orgIDStr := fmt.Sprint(orgID)
			sch.metrics.StarvationDetected.WithLabelValues(orgIDStr).Inc()
			sch.metrics.StarvationDuration.WithLabelValues(orgIDStr).Set(duration.Seconds())
		}

		sch.starvationDetector = NewStarvationDetector(
			cfg.StarvationThreshold,
			cfg.StarvationCheckInterval,
			starvationCallback,
			cfg.Log,
		)
	}

	// Initialize remediation manager
	sch.remediationManager = NewRemediationManager(cfg)

	return &sch
}

func (sch *schedule) Run(ctx context.Context) error {
	sch.log.Info("Starting scheduler", "tickInterval", sch.baseInterval, "maxAttempts", sch.maxAttempts)
	t := ticker.New(sch.clock, sch.baseInterval, sch.metrics.Ticker)
	defer t.Stop()

	// Start starvation detector if enabled
	if sch.starvationDetector != nil {
		go sch.starvationDetector.Start(ctx)
		sch.log.Info("Started starvation detector")
	}

	if err := sch.schedulePeriodic(ctx, t); err != nil {
		sch.log.Error("Failure while running the rule evaluation loop", "error", err)
	}
	return nil
}

// Rules fetches the entire set of rules considered for evaluation by the scheduler on the next tick.
// Such rules are not guaranteed to have been evaluated by the scheduler.
// Rules returns all supplementary metadata for the rules that is stored by the scheduler - namely, the set of folder titles.
func (sch *schedule) Rules() ([]*ngmodels.AlertRule, map[ngmodels.FolderKey]string) {
	return sch.schedulableAlertRules.all()
}

// Status fetches the health of a given scheduled rule, by key.
func (sch *schedule) Status(key ngmodels.AlertRuleKey) (ngmodels.RuleStatus, bool) {
	if rule, ok := sch.registry.get(key); ok {
		return rule.Status(), true
	}
	return ngmodels.RuleStatus{}, false
}

// deleteAlertRule stops evaluation of the rule, deletes it from active rules, and cleans up state cache.
func (sch *schedule) deleteAlertRule(ctx context.Context, keys ...ngmodels.AlertRuleKey) {
	for _, key := range keys {
		// It can happen that the scheduler has deleted the alert rule before the
		// Ruler API has called DeleteAlertRule. This can happen as requests to
		// the Ruler API do not hold an exclusive lock over all scheduler operations.
		_, ok := sch.schedulableAlertRules.del(key)
		if !ok {
			sch.log.Info("Alert rule cannot be removed from the scheduler as it is not scheduled", key.LogContext()...)
		}
		// Delete the rule routine
		ruleRoutine, ok := sch.registry.del(key)
		if !ok {
			sch.log.Info("Alert rule cannot be stopped as it is not running", key.LogContext()...)
			continue
		}

		// stop rule evaluation
		reason := sch.getRuleStopReason(ctx, ruleRoutine.Identifier())
		ruleRoutine.Stop(reason)
	}
	// Our best bet at this point is that we update the metrics with what we hope to schedule in the next tick.
	alertRules, _ := sch.schedulableAlertRules.all()
	sch.updateRulesMetrics(alertRules)
}

func (sch *schedule) getRuleStopReason(ctx context.Context, key ngmodels.AlertRuleKeyWithGroup) error {
	// If the ruleStopReasonProvider is defined, we will use it to get the reason why the
	// alert rule was stopped. If it returns an error, we will use the default reason.
	if sch.ruleStopReasonProvider == nil {
		return errRuleDeleted
	}

	stopReason, err := sch.ruleStopReasonProvider.FindReason(ctx, sch.log, key)
	if err != nil {
		sch.log.New(key.LogContext()...).Error("Failed to get stop reason", "error", err)
		return errRuleDeleted
	}
	if stopReason == nil {
		return errRuleDeleted
	}

	return stopReason
}

func (sch *schedule) schedulePeriodic(ctx context.Context, t *ticker.T) error {
	dispatcherGroup, ctx := errgroup.WithContext(ctx)
	for {
		select {
		case tick := <-t.C:
			// We use Round(0) on the start time to remove the monotonic clock.
			// This is required as ticks from the ticker and time.Now() can have
			// a monotonic clock that when subtracted do not represent the delta
			// in wall clock time.
			start := time.Now().Round(0)
			sch.metrics.BehindSeconds.Set(start.Sub(tick).Seconds())

			sch.processTick(ctx, dispatcherGroup, tick)

			sch.metrics.SchedulePeriodicDuration.Observe(time.Since(start).Seconds())
		case <-ctx.Done():
			// waiting for all rule evaluation routines to stop
			waitErr := dispatcherGroup.Wait()
			return waitErr
		}
	}
}

type readyToRunItem struct {
	ruleRoutine Rule
	Evaluation
}

// TODO refactor to accept a callback for tests that will be called with things that are returned currently, and return nothing.
// Returns a slice of rules that were scheduled for evaluation, map of stopped rules, and a slice of updated rules
func (sch *schedule) processTick(ctx context.Context, dispatcherGroup *errgroup.Group, tick time.Time) ([]readyToRunItem, map[ngmodels.AlertRuleKey]struct{}, []ngmodels.AlertRuleKeyWithVersion) {
	tickNum := tick.Unix() / int64(sch.baseInterval.Seconds())

	// update the local registry. If there was a difference between the previous state and the current new state, rulesDiff will contains keys of rules that were updated.
	rulesDiff, err := sch.updateSchedulableAlertRules(ctx)
	updated := rulesDiff.updated
	if updated == nil { // make sure map is not nil
		updated = map[ngmodels.AlertRuleKey]struct{}{}
	}
	if err != nil {
		sch.log.Error("Failed to update alert rules", "error", err)
	}

	// this is the new current state. rulesDiff contains the previously existing rules that were different between this state and the previous state.
	alertRules, folderTitles := sch.schedulableAlertRules.all()

	// registeredDefinitions is a map used for finding deleted alert rules
	// initially it is assigned to all known alert rules from the previous cycle
	// each alert rule found also in this cycle is removed
	// so, at the end, the remaining registered alert rules are the deleted ones
	registeredDefinitions := sch.registry.keyMap()

	sch.updateRulesMetrics(alertRules)

	readyToRun := make([]readyToRunItem, 0)
	updatedRules := make([]ngmodels.AlertRuleKeyWithVersion, 0, len(updated)) // this is needed for tests only
	restartedRules := make([]Rule, 0)
	missingFolder := make(map[string][]string)
	ruleFactory := newRuleFactory(
		sch.appURL,
		sch.disableGrafanaFolder,
		sch.maxAttempts,
		sch.alertsSender,
		sch.stateManager,
		sch.evaluatorFactory,
		sch.clock,
		sch.rrCfg,
		sch.metrics,
		sch.log,
		sch.tracer,
		sch.recordingWriter,
		sch.evalAppliedFunc,
		sch.stopAppliedFunc,
	)
	for _, item := range alertRules {
		ruleRoutine, newRoutine := sch.registry.getOrCreate(ctx, item, ruleFactory)
		key := item.GetKey()
		logger := sch.log.FromContext(ctx).New(key.LogContext()...)

		// enforce minimum evaluation interval
		if item.IntervalSeconds < int64(sch.minRuleInterval.Seconds()) {
			logger.Debug("Interval adjusted", "originalInterval", item.IntervalSeconds, "adjustedInterval", sch.minRuleInterval.Seconds())
			item.IntervalSeconds = int64(sch.minRuleInterval.Seconds())
		}

		invalidInterval := item.IntervalSeconds%int64(sch.baseInterval.Seconds()) != 0

		if item.Type() != ruleRoutine.Type() {
			// Restart rules that need it. For now we just replace them, we'll shut them down at the end of the tick.
			logger.Debug("Rule restarted because type changed", "old", ruleRoutine.Type(), "new", item.Type())
			restartedRules = append(restartedRules, ruleRoutine)
			sch.registry.del(key)
			ruleRoutine, newRoutine = sch.registry.getOrCreate(ctx, item, ruleFactory)
		}

		if newRoutine && !invalidInterval {
			dispatcherGroup.Go(func() error {
				return ruleRoutine.Run()
			})
		}

		if invalidInterval {
			// this is expected to be always false
			// given that we validate interval during alert rule updates
			logger.Warn("Rule has an invalid interval and will be ignored. Interval should be divided exactly by scheduler interval", "ruleInterval", time.Duration(item.IntervalSeconds)*time.Second, "schedulerInterval", sch.baseInterval)
			continue
		}

		itemFrequency := item.IntervalSeconds / int64(sch.baseInterval.Seconds())
		offset := jitterOffsetInTicks(item, sch.baseInterval, sch.jitterEvaluations)
		isReadyToRun := item.IntervalSeconds != 0 && (tickNum%itemFrequency)-offset == 0

		var folderTitle string
		if !sch.disableGrafanaFolder {
			title, ok := folderTitles[item.GetFolderKey()]
			if ok {
				folderTitle = title
			} else {
				missingFolder[item.NamespaceUID] = append(missingFolder[item.NamespaceUID], item.UID)
			}
		}

		if isReadyToRun {
			logger.Debug("Rule is ready to run on the current tick", "tick", tick, "frequency", itemFrequency, "offset", offset)
			readyToRun = append(readyToRun, readyToRunItem{ruleRoutine: ruleRoutine, Evaluation: Evaluation{
				scheduledAt: tick,
				rule:        item,
				folderTitle: folderTitle,
			}})
		}
		if _, isUpdated := updated[key]; isUpdated && !isReadyToRun {
			// if we do not need to eval the rule, check the whether rule was just updated and if it was, notify evaluation routine about that
			logger.Debug("Rule has been updated. Notifying evaluation routine")
			go func(routine Rule, rule *ngmodels.AlertRule) {
				routine.Update(&Evaluation{
					rule:        rule,
					folderTitle: folderTitle,
				})
			}(ruleRoutine, item)
			updatedRules = append(updatedRules, ngmodels.AlertRuleKeyWithVersion{
				Version:      item.Version,
				AlertRuleKey: item.GetKey(),
			})
		}

		// remove the alert rule from the registered alert rules
		delete(registeredDefinitions, key)
	}

	if len(missingFolder) > 0 { // if this happens then there can be problems with fetching folders from the database.
		sch.log.Warn("Unable to obtain folder titles for some rules", "missingFolderUIDToRuleUID", missingFolder)
	}

	var step int64 = 0
	if len(readyToRun) > 0 {
		step = sch.baseInterval.Nanoseconds() / int64(len(readyToRun))
	}

	slices.SortFunc(readyToRun, func(a, b readyToRunItem) int {
		return strings.Compare(a.rule.UID, b.rule.UID)
	})
	for i := range readyToRun {
		item := readyToRun[i]

		time.AfterFunc(time.Duration(int64(i)*step), func() {
			key := item.rule.GetKey()
			logger := sch.log.FromContext(ctx).New(key.LogContext()...)
			
			// Create context with timeout for evaluation
			evalCtx, cancel := context.WithTimeout(ctx, sch.evaluationTimeout)
			defer cancel()
			
			// Acquire semaphore slot with context support
			releaseSlot, err := sch.orgSemaphoreManager.TryAcquireEvaluationSlot(evalCtx, key.OrgID)
			if err != nil {
				orgID := fmt.Sprint(key.OrgID)
				
				// Generate remediation context
				remediationContext := GetRemediationContext(item.rule.Title, sch.getQueueDepth(key.OrgID))
				suggestion := sch.remediationManager.SuggestRemediation(err, key.OrgID, remediationContext)
				
				// Enhanced error handling with backpressure detection and remediation suggestions
				if err == context.DeadlineExceeded {
					logger.Warn("Evaluation skipped due to timeout", 
						"error", err, 
						"timeout", sch.evaluationTimeout,
						"suggestion", suggestion,
						"backpressure_detected", true)
					sch.metrics.EvaluationSkipped.WithLabelValues(orgID, item.rule.Title, "timeout").Inc()
				} else {
					logger.Warn("Evaluation skipped due to concurrency limit", 
						"error", err,
						"suggestion", suggestion,
						"queue_depth", sch.getQueueDepth(key.OrgID))
					sch.metrics.EvaluationSkipped.WithLabelValues(orgID, item.rule.Title, err.Error()).Inc()
				}
				return
			}
			defer releaseSlot()
			
			success, dropped := item.ruleRoutine.Eval(&item.Evaluation)
			if !success {
				logger.Debug("Scheduled evaluation was canceled because evaluation routine was stopped", append(key.LogContext(), "time", tick)...)
				return
			}
			
			// Record successful evaluation for starvation detection
			if sch.starvationDetector != nil {
				sch.starvationDetector.RecordEvaluation(key.OrgID)
			}
			if dropped != nil {
				logger.Warn("Tick dropped because alert rule evaluation is too slow", append(key.LogContext(), "time", tick, "droppedTick", dropped.scheduledAt)...)
				orgID := fmt.Sprint(key.OrgID)
				sch.metrics.EvaluationMissed.WithLabelValues(orgID, item.rule.Title).Inc()
			}
		})
	}

	// Stop old routines for rules that got restarted.
	for _, oldRoutine := range restartedRules {
		oldRoutine.Stop(errRuleRestarted)
	}

	// unregister and stop routines of the deleted alert rules
	toDelete := make([]ngmodels.AlertRuleKey, 0, len(registeredDefinitions))
	for key := range registeredDefinitions {
		toDelete = append(toDelete, key)
	}
	sch.deleteAlertRule(ctx, toDelete...)
	
	// Update concurrency metrics
	sch.updateConcurrencyMetrics()
	
	return readyToRun, registeredDefinitions, updatedRules
}

// updateConcurrencyMetrics updates Prometheus metrics for concurrency monitoring
func (sch *schedule) updateConcurrencyMetrics() {
	if sch.orgSemaphoreManager == nil {
		return
	}
	
	stats := sch.orgSemaphoreManager.GetRichStats()
	
	// Update global metrics
	if globalActive, ok := stats["global_active"].(int); ok {
		if globalLimit, ok := stats["global_limit"].(int); ok {
			utilization := float64(globalActive) / float64(globalLimit)
			sch.metrics.GlobalConcurrencyUtilization.Set(utilization)
		}
	}
	
	// Update per-org metrics
	if orgDetails, ok := stats["org_details"].(map[int64]map[string]interface{}); ok {
		for orgID, details := range orgDetails {
			orgIDStr := fmt.Sprint(orgID)
			
			if active, ok := details["active"].(int); ok {
				sch.metrics.ActiveEvaluationsPerOrg.WithLabelValues(orgIDStr).Set(float64(active))
				
				if perOrgLimit, ok := stats["per_org_limit"].(int); ok {
					utilization := float64(active) / float64(perOrgLimit)
					sch.metrics.ConcurrencyLimitUtilization.WithLabelValues(orgIDStr, "per_org").Set(utilization)
				}
			}
		}
	}
	
	// Update starvation metrics if detector is enabled
	if sch.starvationDetector != nil {
		starvationStatus := sch.starvationDetector.GetStarvationStatus()
		for orgID, sinceLastEval := range starvationStatus {
			orgIDStr := fmt.Sprint(orgID)
			sch.metrics.LastEvaluationAge.WithLabelValues(orgIDStr).Set(sinceLastEval.Seconds())
		}
	}
}

// getQueueDepth returns the current queue depth for an organization
func (sch *schedule) getQueueDepth(orgID int64) int {
	if sch.orgSemaphoreManager == nil {
		return 0
	}
	
	stats := sch.orgSemaphoreManager.GetRichStats()
	if orgDetails, ok := stats["org_details"].(map[int64]map[string]interface{}); ok {
		if orgData, exists := orgDetails[orgID]; exists {
			if queueDepth, ok := orgData["queue_depth"].(int); ok {
				return queueDepth
			}
		}
	}
	return 0
}
