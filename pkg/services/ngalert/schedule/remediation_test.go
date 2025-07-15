package schedule

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRemediationManager_SuggestRemediation(t *testing.T) {
	cfg := SchedulerCfg{
		MaxEvaluationConcurrency:       50,
		MaxEvaluationConcurrencyPerOrg: 10,
		EvaluationTimeout:              30 * time.Second,
	}
	
	rm := NewRemediationManager(cfg)

	tests := []struct {
		name             string
		err              error
		orgID            int64
		context          map[string]interface{}
		expectedContains []string
	}{
		{
			name:    "timeout error with high queue depth",
			err:     context.DeadlineExceeded,
			orgID:   1,
			context: map[string]interface{}{"queue_depth": 10, "rule_name": "test-rule"},
			expectedContains: []string{
				"consider:",
				"increase evaluation_timeout",
				"increase per-org concurrency limit",
				"optimize rule 'test-rule'",
			},
		},
		{
			name:    "timeout error with low queue depth",
			err:     context.DeadlineExceeded,
			orgID:   1,
			context: map[string]interface{}{"queue_depth": 2},
			expectedContains: []string{
				"consider:",
				"increase evaluation_timeout",
			},
		},
		{
			name:    "per-org concurrency limit error",
			err:     errors.New("organization evaluation concurrency limit reached"),
			orgID:   1,
			context: map[string]interface{}{},
			expectedContains: []string{
				"consider:",
				"increase per-org concurrency limit from 10 to 12",
				"review rule complexity for org 1",
				"staggering rule evaluation intervals",
			},
		},
		{
			name:    "global concurrency limit error",
			err:     errors.New("global evaluation concurrency limit reached"),
			orgID:   1,
			context: map[string]interface{}{},
			expectedContains: []string{
				"consider:",
				"increase global concurrency limit from 50 to 60",
				"monitor CPU and memory usage",
				"scaling horizontally",
			},
		},
		{
			name:    "unknown error",
			err:     errors.New("some unknown error"),
			orgID:   1,
			context: map[string]interface{}{},
			expectedContains: []string{
				"Check system resources and rule complexity",
			},
		},
		{
			name:             "nil error",
			err:              nil,
			orgID:            1,
			context:          map[string]interface{}{},
			expectedContains: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			suggestion := rm.SuggestRemediation(tt.err, tt.orgID, tt.context)
			
			if len(tt.expectedContains) == 0 {
				assert.Empty(t, suggestion, "Should return empty suggestion for nil error")
				return
			}

			for _, expected := range tt.expectedContains {
				assert.Contains(t, suggestion, expected, "Suggestion should contain expected text")
			}
		})
	}
}

func TestRemediationManager_TimeoutRemediation(t *testing.T) {
	cfg := SchedulerCfg{
		MaxEvaluationConcurrencyPerOrg: 5,
		EvaluationTimeout:              15 * time.Second, // Short timeout
	}
	
	rm := NewRemediationManager(cfg)

	// Test timeout remediation with short timeout
	suggestion := rm.suggestTimeoutRemediation(1, map[string]interface{}{})
	
	assert.Contains(t, suggestion, "increase evaluation_timeout from 15s to 30s")
	assert.Contains(t, suggestion, "consider:")
}

func TestRemediationManager_PerOrgConcurrencyRemediation(t *testing.T) {
	cfg := SchedulerCfg{
		MaxEvaluationConcurrencyPerOrg: 8,
	}
	
	rm := NewRemediationManager(cfg)

	suggestion := rm.suggestPerOrgConcurrencyRemediation(1, map[string]interface{}{})
	
	// Should suggest increasing from 8 to 10 (8 + max(2, 8/4) = 8 + 2 = 10)
	assert.Contains(t, suggestion, "increase per-org concurrency limit from 8 to 10")
	assert.Contains(t, suggestion, "review rule complexity for org 1")
	assert.Contains(t, suggestion, "consider:")
}

func TestRemediationManager_GlobalConcurrencyRemediation(t *testing.T) {
	cfg := SchedulerCfg{
		MaxEvaluationConcurrency: 40,
	}
	
	rm := NewRemediationManager(cfg)

	suggestion := rm.suggestGlobalConcurrencyRemediation(map[string]interface{}{})
	
	// Should suggest increasing from 40 to 50 (40 + max(10, 40/5) = 40 + 10 = 50)
	assert.Contains(t, suggestion, "increase global concurrency limit from 40 to 50")
	assert.Contains(t, suggestion, "monitor CPU and memory usage")
	assert.Contains(t, suggestion, "consider:")
}

func TestIsTimeoutError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "context deadline exceeded",
			err:      context.DeadlineExceeded,
			expected: true,
		},
		{
			name:     "timeout in error message",
			err:      errors.New("operation timeout"),
			expected: true,
		},
		{
			name:     "deadline exceeded in error message",
			err:      errors.New("deadline exceeded while waiting"),
			expected: true,
		},
		{
			name:     "context canceled in error message",
			err:      errors.New("context canceled"),
			expected: true,
		},
		{
			name:     "other error",
			err:      errors.New("some other error"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isTimeoutError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetRemediationContext(t *testing.T) {
	tests := []struct {
		name         string
		ruleName     string
		queueDepth   int
		expectedKeys []string
	}{
		{
			name:         "with rule name and queue depth",
			ruleName:     "test-rule",
			queueDepth:   5,
			expectedKeys: []string{"rule_name", "queue_depth"},
		},
		{
			name:         "with rule name only",
			ruleName:     "test-rule",
			queueDepth:   -1,
			expectedKeys: []string{"rule_name"},
		},
		{
			name:         "with queue depth only",
			ruleName:     "",
			queueDepth:   3,
			expectedKeys: []string{"queue_depth"},
		},
		{
			name:         "empty context",
			ruleName:     "",
			queueDepth:   -1,
			expectedKeys: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			context := GetRemediationContext(tt.ruleName, tt.queueDepth)
			
			assert.Len(t, context, len(tt.expectedKeys), "Context should have expected number of keys")
			
			for _, key := range tt.expectedKeys {
				assert.Contains(t, context, key, "Context should contain expected key")
			}
			
			if tt.ruleName != "" {
				assert.Equal(t, tt.ruleName, context["rule_name"])
			}
			
			if tt.queueDepth >= 0 {
				assert.Equal(t, tt.queueDepth, context["queue_depth"])
			}
		})
	}
}

func TestMaxFunction(t *testing.T) {
	tests := []struct {
		name     string
		a        int
		b        int
		expected int
	}{
		{"a greater than b", 10, 5, 10},
		{"b greater than a", 3, 8, 8},
		{"a equal to b", 7, 7, 7},
		{"negative numbers", -5, -2, -2},
		{"zero values", 0, 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := max(tt.a, tt.b)
			assert.Equal(t, tt.expected, result)
		})
	}
}