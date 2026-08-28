package scoring

import (
	"fmt"
	"testing"
)

func TestModelLoadAndScore(t *testing.T) {
	// Load the XGBoost model from native JSON
	model, err := LoadModel("../../models/fraud_xgboost_v1.0.0.json")
	if err != nil {
		t.Fatalf("Failed to load model: %v", err)
	}
	t.Logf("✅ Model loaded: %d trees, base_score=%.4f", model.NumTrees(), model.BaseScore())

	// Load the StandardScaler
	scaler, err := LoadScaler("../../models/scaler_v1.0.0.json")
	if err != nil {
		t.Fatalf("Failed to load scaler: %v", err)
	}
	t.Logf("✅ Scaler loaded: %d features", scaler.NFeatures)
	model.SetScaler(scaler)

	// Legitimate transaction
	legitFeatures := map[string]string{
		"velocity_tx_count_1h":              "2",
		"velocity_tx_count_24h":             "8",
		"velocity_amount_sum_1h":            "200",
		"velocity_amount_sum_24h":           "1500",
		"velocity_decline_count_24h":        "0",
		"velocity_unique_countries_1h":      "1",
		"velocity_unique_merchants_24h":     "4",
		"velocity_avg_amount_7d":            "250",
		"velocity_stddev_amount_7d":         "80",
		"velocity_time_since_last_tx":       "3600",
		"behavioral_typical_amount_ratio":   "1.1",
		"behavioral_typical_hour_score":     "0.8",
		"behavioral_typical_day_score":      "0.6",
		"behavioral_merchant_category_diversity": "8",
		"behavioral_amount_zscore":          "0.3",
		"behavioral_is_recipient_new":       "0",
		"behavioral_velocity_direction":     "1.0",
		"behavioral_time_between_tx_stddev": "1800",
		"behavioral_country_change_freq":    "0.1",
		"behavioral_night_tx_ratio":         "0.1",
		"device_is_known":                   "1",
		"device_last_seen_hours_ago":        "24",
		"device_unique_accounts_24h":        "1",
		"device_is_emulator_detected":       "0",
		"device_rooted_jailbroken":          "0",
		"device_ip_country_match":           "1",
		"device_ip_is_vpn":                  "0",
		"device_browser_fingerprint_match":  "1",
		"device_latency_anomaly":            "0",
		"device_is_new_os_version":          "0",
	}

	prob, topFeatures := model.Predict(legitFeatures)
	fmt.Printf("\n🟢 Legit transaction:\n")
	fmt.Printf("   Fraud probability: %.6f\n", prob)
	fmt.Printf("   Decision: %s\n", classify(prob))
	fmt.Printf("   Top features: %v\n", topFeatures)

	if prob >= 0.70 {
		t.Errorf("Legit transaction should not be DECLINED, got prob=%.4f", prob)
	}

	// Fraudulent transaction
	fraudFeatures := map[string]string{
		"velocity_tx_count_1h":              "15",
		"velocity_tx_count_24h":             "50",
		"velocity_amount_sum_1h":            "5000",
		"velocity_amount_sum_24h":           "25000",
		"velocity_decline_count_24h":        "4",
		"velocity_unique_countries_1h":      "4",
		"velocity_unique_merchants_24h":     "25",
		"velocity_avg_amount_7d":            "100",
		"velocity_stddev_amount_7d":         "500",
		"velocity_time_since_last_tx":       "200",
		"behavioral_typical_amount_ratio":   "15",
		"behavioral_typical_hour_score":     "0.1",
		"behavioral_typical_day_score":      "0.1",
		"behavioral_merchant_category_diversity": "30",
		"behavioral_amount_zscore":          "5.0",
		"behavioral_is_recipient_new":       "1",
		"behavioral_velocity_direction":     "5.0",
		"behavioral_time_between_tx_stddev": "300",
		"behavioral_country_change_freq":    "3.0",
		"behavioral_night_tx_ratio":         "0.8",
		"device_is_known":                   "0",
		"device_last_seen_hours_ago":        "500",
		"device_unique_accounts_24h":        "8",
		"device_is_emulator_detected":       "1",
		"device_rooted_jailbroken":          "1",
		"device_ip_country_match":           "0",
		"device_ip_is_vpn":                  "1",
		"device_browser_fingerprint_match":  "0",
		"device_latency_anomaly":            "1",
		"device_is_new_os_version":          "1",
	}

	prob2, topFeatures2 := model.Predict(fraudFeatures)
	fmt.Printf("\n🔴 Fraud transaction:\n")
	fmt.Printf("   Fraud probability: %.6f\n", prob2)
	fmt.Printf("   Decision: %s\n", classify(prob2))
	fmt.Printf("   Top features: %v\n", topFeatures2)

	if prob2 < 0.70 {
		t.Errorf("Fraud transaction should be DECLINED, got prob=%.4f", prob2)
	}
}

func classify(p float64) string {
	if p < 0.30 {
		return "APPROVE"
	}
	if p < 0.70 {
		return "REVIEW"
	}
	return "DECLINE"
}
