package scoring

import (
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
)

// Decision represents the fraud assessment outcome.
type Decision int

const (
	DecisionApprove Decision = iota
	DecisionReview
	DecisionDecline
)

func (d Decision) String() string {
	switch d {
	case DecisionApprove:
		return "APPROVE"
	case DecisionReview:
		return "REVIEW"
	case DecisionDecline:
		return "DECLINE"
	default:
		return "UNKNOWN"
	}
}

// ScoreResult holds the complete scoring result.
type ScoreResult struct {
	TransactionID     string
	Decision          Decision
	FraudProbability  float64
	ModelVersion      string
	LatencyMs         int64
	TopFeatures       map[string]float64
	ReasonCode        string
	TimestampMs       int64
}

// ReasonCode represents predefined reason codes for decisions.
const (
	ReasonCodeUnknown              = "UNKNOWN"
	ReasonCodeVelocityBurst        = "VELOCITY_BURST"
	ReasonCodeNewDevice            = "NEW_DEVICE"
	ReasonCodeAmountAnomaly        = "AMOUNT_ANOMALY"
	ReasonCodeGeoAnomaly           = "GEO_ANOMALY"
	ReasonCodeImpossibleTravel     = "IMPOSSIBLE_TRAVEL"
	ReasonCodeVPNProxy             = "VPN_PROXY"
	ReasonCodeEmulatorDetected     = "EMULATOR_DETECTED"
	ReasonCodeHighRiskMCC          = "HIGH_RISK_MCC"
	ReasonCodeModelScoreHigh       = "MODEL_SCORE_HIGH"
	ReasonCodeModelScoreMedium     = "MODEL_SCORE_MEDIUM"
	ReasonCodeRuleViolation        = "RULE_VIOLATION"
	ReasonCodeBlockedCountry       = "BLOCKED_COUNTRY"
)

// Scorer orchestrates model prediction and rule-based fallback.
type Scorer struct {
	model     *Model
	rules     *RulesEngine
	thresholds Thresholds
}

// NewScorer creates a new Scorer with the given model and rules.
func NewScorer(model *Model, rules *RulesEngine) *Scorer {
	return &Scorer{
		model:      model,
		rules:      rules,
		thresholds: model.Thresholds,
	}
}

// Score evaluates a transaction and returns a ScoreResult.
func (s *Scorer) Score(transactionID string, features map[string]string, modelVersion string, timestampMs int64) *ScoreResult {
	start := time.Now()

	// 1. Try XGBoost model prediction
	probability, topFeatures := s.model.Predict(features)

	// 2. Determine decision from threshold
	decision, reasonCode := s.classify(probability, features)

	// 3. Apply rule-based overrides (can only escalate, never down-grade)
	ruleDecision, ruleReason := s.rules.Evaluate(features)
	if ruleDecision > decision {
		decision = ruleDecision
		reasonCode = ruleReason
	}

	latencyMs := time.Since(start).Milliseconds()

	result := &ScoreResult{
		TransactionID:    transactionID,
		Decision:         decision,
		FraudProbability: probability,
		ModelVersion:     modelVersion,
		LatencyMs:        latencyMs,
		TopFeatures:      topFeatures,
		ReasonCode:       reasonCode,
		TimestampMs:      timestampMs,
	}

	log.Info().
		Str("tx_id", transactionID).
		Float64("probability", probability).
		Str("decision", decision.String()).
		Str("reason", reasonCode).
		Int64("latency_ms", latencyMs).
		Msg("Transaction scored")

	return result
}

// classify maps probability to decision and assigns reason code.
func (s *Scorer) classify(probability float64, features map[string]string) (Decision, string) {
	if probability < s.thresholds.Approve {
		return DecisionApprove, ReasonCodeUnknown
	}

	if probability < s.thresholds.Review {
		reason := s.identifyReviewReason(features)
		return DecisionReview, reason
	}

	reason := s.identifyDeclineReason(features, probability)
	return DecisionDecline, reason
}

// identifyReviewReason determines why a transaction is flagged for review.
func (s *Scorer) identifyReviewReason(features map[string]string) string {
	txCount1h := parseFeatureInt(features, "velocity_tx_count_1h")
	deviceKnown := parseFeatureInt(features, "device_is_known")
	countryMatch := parseFeatureInt(features, "device_ip_country_match")

	if txCount1h > 5 {
		return ReasonCodeVelocityBurst
	}
	if deviceKnown == 0 {
		return ReasonCodeNewDevice
	}
	if countryMatch == 0 {
		return ReasonCodeGeoAnomaly
	}
	return ReasonCodeModelScoreMedium
}

// identifyDeclineReason determines why a transaction is declined.
func (s *Scorer) identifyDeclineReason(features map[string]string, probability float64) string {
	txCount1h := parseFeatureInt(features, "velocity_tx_count_1h")
	deviceKnown := parseFeatureInt(features, "device_is_known")
	emulator := parseFeatureInt(features, "device_is_emulator_detected")
	countries1h := parseFeatureInt(features, "velocity_unique_countries_1h")
	vpn := parseFeatureInt(features, "device_ip_is_vpn")

	if txCount1h > 10 {
		return ReasonCodeVelocityBurst
	}
	if countries1h > 2 {
		return ReasonCodeImpossibleTravel
	}
	if emulator == 1 {
		return ReasonCodeEmulatorDetected
	}
	if deviceKnown == 0 && vpn == 1 {
		return ReasonCodeVPNProxy
	}
	return ReasonCodeModelScoreHigh
}

func parseFeatureInt(features map[string]string, key string) int {
	v, ok := features[key]
	if !ok {
		return 0
	}
	var n int
	for _, c := range v {
		if c >= '0' && c <= '9' {
			n = n*10 + int(c-'0')
		}
	}
	return n
}

// FormatDecision returns a human-readable decision string.
func FormatDecision(r *ScoreResult) string {
	return fmt.Sprintf(
		"TX=%s | Decision=%s | Probability=%.4f | Model=%s | Latency=%dms | Reason=%s",
		r.TransactionID, r.Decision, r.FraudProbability,
		r.ModelVersion, r.LatencyMs, r.ReasonCode,
	)
}
