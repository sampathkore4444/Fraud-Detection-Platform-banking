package rules

import (
	"math"
	"time"
)

// RulesEngine provides rule-based fraud detection as a fallback
// when the ML model is unavailable or for hard business rules.
type RulesEngine struct {
	config RulesConfig
}

type RulesConfig struct {
	Enabled             bool
	MaxAmountPerDay     float64
	MaxTxPerHour        int
	MaxCountriesPerDay  int
	BlockedCountries    []string
	MaxAmountSingleTx   float64
	MinTimeBetweenTxSec int64
}

// RuleResult represents the outcome of rule evaluation.
type RuleResult struct {
	Violated bool
	RuleName string
	Severity int // 0=none, 1=review, 2=decline
	Message  string
}

// NewRulesEngine creates a rules engine with the given configuration.
func NewRulesEngine(config RulesConfig) *RulesEngine {
	return &RulesEngine{config: config}
}

// Evaluate applies all rules to the feature vector.
// Returns the highest-severity decision and reason code.
func (r *RulesEngine) Evaluate(features map[string]string) (Decision, string) {
	if !r.config.Enabled {
		return DecisionApprove, ""
	}

	violations := []RuleResult{}

	// Rule 1: Blocked countries
	if result := r.checkBlockedCountry(features); result.Violated {
		violations = append(violations, result)
	}

	// Rule 2: Velocity burst — too many transactions in 1 hour
	if result := r.checkVelocityBurst(features); result.Violated {
		violations = append(violations, result)
	}

	// Rule 3: Impossible travel — too many countries in 1 hour
	if result := r.checkImpossibleTravel(features); result.Violated {
		violations = append(violations, result)
	}

	// Rule 4: Amount anomaly — single transaction too large
	if result := r.checkAmountAnomaly(features); result.Violated {
		violations = append(violations, result)
	}

	// Rule 5: Known emulator detected
	if result := r.checkEmulator(features); result.Violated {
		violations = append(violations, result)
	}

	// Rule 6: Multiple new devices in short time
	if result := r.checkDeviceAnomaly(features); result.Violated {
		violations = append(violations, result)
	}

	// Return highest severity
	return pickHighestSeverity(violations)
}

func (r *RulesEngine) checkBlockedCountry(features map[string]string) RuleResult {
	country := features["country_code"]
	for _, blocked := range r.config.BlockedCountries {
		if country == blocked {
			return RuleResult{
				Violated: true,
				RuleName: "blocked_country",
				Severity: 2,
				Message:  "Transaction from blocked country: " + country,
			}
		}
	}
	return RuleResult{}
}

func (r *RulesEngine) checkVelocityBurst(features map[string]string) RuleResult {
	txCount1h := parseInt(features, "velocity_tx_count_1h")
	if txCount1h > r.config.MaxTxPerHour {
		return RuleResult{
			Violated: true,
			RuleName: "velocity_burst",
			Severity: 2,
			Message:  "Transaction count in 1h exceeds limit",
		}
	}
	if txCount1h > r.config.MaxTxPerHour/2 {
		return RuleResult{
			Violated: true,
			RuleName: "velocity_elevated",
			Severity: 1,
			Message:  "Elevated transaction velocity",
		}
	}
	return RuleResult{}
}

func (r *RulesEngine) checkImpossibleTravel(features map[string]string) RuleResult {
	countries1h := parseInt(features, "velocity_unique_countries_1h")
	if countries1h > r.config.MaxCountriesPerDay {
		return RuleResult{
			Violated: true,
			RuleName: "impossible_travel",
			Severity: 2,
			Message:  "Too many distinct countries in 1 hour",
		}
	}
	if countries1h > 1 {
		return RuleResult{
			Violated: true,
			RuleName: "multi_country",
			Severity: 1,
			Message:  "Multiple countries in short time window",
		}
	}
	return RuleResult{}
}

func (r *RulesEngine) checkAmountAnomaly(features map[string]string) RuleResult {
	amountSum1h := parseFloat(features, "velocity_amount_sum_1h")
	if r.config.MaxAmountSingleTx > 0 && amountSum1h > r.config.MaxAmountSingleTx {
		return RuleResult{
			Violated: true,
			RuleName: "amount_anomaly",
			Severity: 2,
			Message:  "Single transaction amount exceeds threshold",
		}
	}

	zscore := parseFloat(features, "behavioral_amount_zscore")
	if math.Abs(zscore) > 4.0 {
		return RuleResult{
			Violated: true,
			RuleName: "amount_zscore",
			Severity: 1,
			Message:  "Amount z-score exceeds threshold",
		}
	}
	return RuleResult{}
}

func (r *RulesEngine) checkEmulator(features map[string]string) RuleResult {
	emulator := parseInt(features, "device_is_emulator_detected")
	if emulator == 1 {
		return RuleResult{
			Violated: true,
			RuleName: "emulator_detected",
			Severity: 2,
			Message:  "Known emulator fingerprint detected",
		}
	}
	rooted := parseInt(features, "device_rooted_jailbroken")
	if rooted == 1 {
		return RuleResult{
			Violated: true,
			RuleName: "rooted_device",
			Severity: 2,
			Message:  "Rooted/jailbroken device detected",
		}
	}
	return RuleResult{}
}

func (r *RulesEngine) checkDeviceAnomaly(features map[string]string) RuleResult {
	uniqueAccounts := parseInt(features, "device_unique_accounts_24h")
	if uniqueAccounts > 5 {
		return RuleResult{
			Violated: true,
			RuleName: "device_multi_account",
			Severity: 2,
			Message:  "Too many accounts on same device in 24h",
		}
	}
	if uniqueAccounts > 2 {
		return RuleResult{
			Violated: true,
			RuleName: "device_elevated_accounts",
			Severity: 1,
			Message:  "Multiple accounts on same device",
		}
	}
	return RuleResult{}
}

func pickHighestSeverity(violations []RuleResult) (Decision, string) {
	maxSeverity := 0
	reason := ""
	for _, v := range violations {
		if v.Severity > maxSeverity {
			maxSeverity = v.Severity
			reason = v.RuleName
		}
	}

	switch maxSeverity {
	case 2:
		return DecisionDecline, reason
	case 1:
		return DecisionReview, reason
	default:
		return DecisionApprove, ""
	}
}

// Decision mirrors scoring.Decision to avoid circular imports.
type Decision int

const (
	DecisionApprove Decision = iota
	DecisionReview
	DecisionDecline
)

func parseInt(features map[string]string, key string) int {
	v, ok := features[key]
	if !ok {
		return 0
	}
	n := 0
	for _, c := range v {
		if c >= '0' && c <= '9' {
			n = n*10 + int(c-'0')
		}
	}
	return n
}

func parseFloat(features map[string]string, key string) float64 {
	v, ok := features[key]
	if !ok {
		return 0
	}
	var result float64
	var neg bool
	var afterDot bool
	div := 1.0

	for _, c := range v {
		if c == '-' {
			neg = true
			continue
		}
		if c == '.' {
			afterDot = true
			continue
		}
		if c >= '0' && c <= '9' {
			d := float64(c - '0')
			if afterDot {
				div *= 10
				result += d / div
			} else {
				result = result*10 + d
			}
		}
	}
	if neg {
		return -result
	}
	return result
}

// Ensure we use time even if not directly referenced (for future rate limiting).
var _ = time.Now
