package schedule

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// RemediationManager provides actionable suggestions for common evaluation issues
type RemediationManager struct {
	currentConfig SchedulerCfg
}

// NewRemediationManager creates a new remediation manager
func NewRemediationManager(cfg SchedulerCfg) *RemediationManager {
	return &RemediationManager{
		currentConfig: cfg,
	}
}

// SuggestRemediation provides actionable suggestions for evaluation errors
func (rm *RemediationManager) SuggestRemediation(err error, orgID int64, context map[string]interface{}) string {
	if err == nil {
		return ""
	}

	errMsg := err.Error()
	
	switch {
	case isTimeoutError(err):
		return rm.suggestTimeoutRemediation(orgID, context)
	case strings.Contains(errMsg, "organization evaluation concurrency limit reached"):
		return rm.suggestPerOrgConcurrencyRemediation(orgID, context)
	case strings.Contains(errMsg, "global evaluation concurrency limit reached"):
		return rm.suggestGlobalConcurrencyRemediation(context)
	default:
		return "Check system resources and rule complexity"
	}
}

// isTimeoutError checks if the error is a timeout-related error
func isTimeoutError(err error) bool {
	if err == context.DeadlineExceeded {
		return true
	}
	errMsg := err.Error()
	return strings.Contains(errMsg, "timeout") || 
		   strings.Contains(errMsg, "deadline exceeded") ||
		   strings.Contains(errMsg, "context canceled")
}

// suggestTimeoutRemediation provides suggestions for timeout-related issues
func (rm *RemediationManager) suggestTimeoutRemediation(orgID int64, context map[string]interface{}) string {
	currentTimeout := rm.currentConfig.EvaluationTimeout
	suggestions := []string{}

	// Suggest increasing evaluation timeout
	if currentTimeout < 60*time.Second {
		newTimeout := currentTimeout + 15*time.Second
		suggestions = append(suggestions, 
			fmt.Sprintf("increase evaluation_timeout from %v to %v", 
				currentTimeout, newTimeout))
	}

	// Check if we have queue depth information
	if queueDepth, ok := context["queue_depth"].(int); ok && queueDepth > 5 {
		newLimit := rm.currentConfig.MaxEvaluationConcurrencyPerOrg + 5
		suggestions = append(suggestions, 
			fmt.Sprintf("increase per-org concurrency limit from %d to %d", 
				rm.currentConfig.MaxEvaluationConcurrencyPerOrg, newLimit))
	}

	// Suggest rule optimization
	if ruleName, ok := context["rule_name"].(string); ok {
		suggestions = append(suggestions,
			fmt.Sprintf("optimize rule '%s' query complexity or reduce data range", ruleName))
	}

	if len(suggestions) == 0 {
		return "system may be under heavy load; monitor CPU/memory usage and rule complexity"
	}

	return "consider: " + strings.Join(suggestions, ", ")
}

// suggestPerOrgConcurrencyRemediation provides suggestions for per-org concurrency issues
func (rm *RemediationManager) suggestPerOrgConcurrencyRemediation(orgID int64, context map[string]interface{}) string {
	currentLimit := rm.currentConfig.MaxEvaluationConcurrencyPerOrg
	
	// Suggest increasing the per-org limit
	newLimit := currentLimit + max(2, currentLimit/4) // Increase by 25% or minimum 2
	suggestions := []string{
		fmt.Sprintf("increase per-org concurrency limit from %d to %d", currentLimit, newLimit),
	}

	// Suggest checking rule complexity
	suggestions = append(suggestions, 
		fmt.Sprintf("review rule complexity for org %d", orgID))

	// Suggest staggering rule intervals
	suggestions = append(suggestions,
		"consider staggering rule evaluation intervals to spread load")

	return "consider: " + strings.Join(suggestions, ", ")
}

// suggestGlobalConcurrencyRemediation provides suggestions for global concurrency issues
func (rm *RemediationManager) suggestGlobalConcurrencyRemediation(context map[string]interface{}) string {
	currentLimit := rm.currentConfig.MaxEvaluationConcurrency
	
	// Suggest increasing the global limit
	newLimit := currentLimit + max(10, currentLimit/5) // Increase by 20% or minimum 10
	suggestions := []string{
		fmt.Sprintf("increase global concurrency limit from %d to %d", currentLimit, newLimit),
	}

	// Suggest checking system resources
	suggestions = append(suggestions, 
		"monitor CPU and memory usage",
		"consider scaling horizontally with multiple Grafana instances")

	return "consider: " + strings.Join(suggestions, ", ")
}

// max returns the maximum of two integers
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// GetRemediationContext creates a context map for remediation suggestions
func GetRemediationContext(ruleName string, queueDepth int) map[string]interface{} {
	context := make(map[string]interface{})
	
	if ruleName != "" {
		context["rule_name"] = ruleName
	}
	
	if queueDepth >= 0 {
		context["queue_depth"] = queueDepth
	}
	
	return context
}