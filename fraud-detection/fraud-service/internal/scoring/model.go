package scoring

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// Model wraps an XGBoost model loaded from a JSON file.
// In production, this would use the native XGBoost Go bindings or ONNX Runtime.
type Model struct {
	Version     string
	LoadedAt    time.Time
	Thresholds  Thresholds
	features    []string
	treeEnsemble TreeEnsemble
	mu          sync.RWMutex
}

type Thresholds struct {
	Approve float64 // 0.00 – approve_threshold → APPROVE
	Review  float64 // approve_threshold – review_threshold → REVIEW
	Decline float64 // review_threshold – 1.0 → DECLINE
}

// TreeEnsemble represents a simplified XGBoost model structure.
// In production, load from native binary or ONNX.
type TreeEnsemble struct {
	NumTrees   int       `json:"num_trees"`
	NumFeatures int      `json:"num_features"`
	Trees      []Tree    `json:"trees"`
	Bias       float64   `json:"bias"`
	ScalePosWeight float64 `json:"scale_pos_weight"`
	FeatureImportance map[string]float64 `json:"feature_importance"`
}

type Tree struct {
	Depth    int       `json:"depth"`
	Nodes    []Node    `json:"nodes"`
	Weight   float64   `json:"weight"`
}

type Node struct {
	FeatureIdx int     `json:"feature_idx"`
	Threshold  float64 `json:"threshold"`
	Left       int     `json:"left"`
	Right      int     `json:"right"`
	Value      float64 `json:"value"`
	IsLeaf     bool    `json:"is_leaf"`
}

// Default model for demonstration purposes.
// In production, load from trained model binary.
var defaultFeatures = []string{
	"velocity_tx_count_1h",
	"velocity_tx_count_24h",
	"velocity_amount_sum_1h",
	"velocity_amount_sum_24h",
	"velocity_decline_count_24h",
	"velocity_unique_countries_1h",
	"velocity_unique_merchants_24h",
	"velocity_avg_amount_7d",
	"velocity_stddev_amount_7d",
	"velocity_time_since_last_tx",
	"behavioral_typical_amount_ratio",
	"behavioral_typical_hour_score",
	"behavioral_typical_day_score",
	"behavioral_merchant_category_diversity",
	"behavioral_amount_zscore",
	"behavioral_is_recipient_new",
	"behavioral_velocity_direction",
	"behavioral_time_between_tx_stddev",
	"behavioral_country_change_freq",
	"behavioral_night_tx_ratio",
	"device_is_known",
	"device_last_seen_hours_ago",
	"device_unique_accounts_24h",
	"device_is_emulator_detected",
	"device_rooted_jailbroken",
	"device_ip_country_match",
	"device_ip_is_vpn",
	"device_browser_fingerprint_match",
	"device_latency_anomaly",
	"device_is_new_os_version",
}

// NewModel creates a model with default thresholds.
func NewModel() *Model {
	return &Model{
		Version:    "v1.0.0",
		LoadedAt:   time.Now(),
		Thresholds: Thresholds{
			Approve: 0.30,
			Review:  0.70,
			Decline: 1.00,
		},
		features: defaultFeatures,
		treeEnsemble: TreeEnsemble{
			NumTrees:    100,
			NumFeatures: len(defaultFeatures),
			Bias:        -2.0,
			FeatureImportance: map[string]float64{
				"velocity_tx_count_1h":       0.15,
				"device_is_known":            0.12,
				"behavioral_amount_zscore":   0.11,
				"device_ip_country_match":    0.10,
				"velocity_unique_countries_1h": 0.09,
			},
		},
	}
}

// LoadModel loads a trained model from a JSON file.
func LoadModel(path string) (*Model, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read model file: %w", err)
	}

	var ensemble TreeEnsemble
	if err := json.Unmarshal(data, &ensemble); err != nil {
		return nil, fmt.Errorf("failed to parse model file: %w", err)
	}

	m := &Model{
		Version:      "v1.0.0",
		LoadedAt:     time.Now(),
		Thresholds:   Thresholds{Approve: 0.30, Review: 0.70, Decline: 1.00},
		features:     defaultFeatures,
		treeEnsemble: ensemble,
	}

	log.Info().
		Int("num_trees", ensemble.NumTrees).
		Int("num_features", ensemble.NumFeatures).
		Msg("Model loaded successfully")

	return m, nil
}

// Predict computes fraud probability from feature vector.
// Returns probability [0, 1] and top feature contributions.
func (m *Model) Predict(features map[string]string) (float64, map[string]float64) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Convert string features to float values
	floatFeatures := m.extractFeatures(features)

	// Compute score using logistic function on tree ensemble
	logit := m.treeEnsemble.Bias

	// Simplified tree scoring — in production use native XGBoost
	for _, tree := range m.treeEnsemble.Trees {
		logit += tree.Predict(floatFeatures) * tree.Weight
	}

	// Apply logistic sigmoid
	probability := sigmoid(logit)

	// Clamp to [0, 1]
	probability = math.Max(0.0, math.Min(1.0, probability))

	// Compute feature contributions for explainability
	topFeatures := m.computeFeatureContributions(floatFeatures)

	return probability, topFeatures
}

// PredictRaw returns the raw logit without sigmoid (for debugging).
func (m *Model) PredictRaw(features map[string]string) float64 {
	floatFeatures := m.extractFeatures(features)
	logit := m.treeEnsemble.Bias
	for _, tree := range m.treeEnsemble.Trees {
		logit += tree.Predict(floatFeatures) * tree.Weight
	}
	return logit
}

func (m *Model) extractFeatures(features map[string]string) []float64 {
	vals := make([]float64, len(m.features))
	for i, name := range m.features {
		if v, ok := features[name]; ok {
			vals[i] = parseFloat(v)
		}
	}
	return vals
}

func (m *Model) computeFeatureContributions(features []float64) map[string]float64 {
	contributions := make(map[string]float64)
	for name, importance := range m.treeEnsemble.FeatureImportance {
		for i, fname := range m.features {
			if fname == name {
				contributions[name] = importance * features[i]
				break
			}
		}
	}
	return contributions
}

// GetTopFeatures returns the top N features by importance.
func (m *Model) GetTopFeatures(features map[string]string, n int) map[string]float64 {
	_, topFeatures := m.Predict(features)

	type kv struct {
		Key   string
		Value float64
	}

	var sorted []kv
	for k, v := range topFeatures {
		sorted = append(sorted, kv{k, math.Abs(v)})
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Value > sorted[j].Value
	})

	result := make(map[string]float64)
	for i, kv := range sorted {
		if i >= n {
			break
		}
		result[kv.Key] = topFeatures[kv.Key]
	}
	return result
}

// sigmoid applies the logistic function.
func sigmoid(x float64) float64 {
	return 1.0 / (1.0 + math.Exp(-x))
}

// Tree.Predict evaluates a single tree on features.
func (t Tree) Predict(features []float64) float64 {
	if len(t.Nodes) == 0 {
		return 0
	}
	node := t.Nodes[0]
	for !node.IsLeaf {
		if features[node.FeatureIdx] <= node.Threshold {
			node = t.Nodes[node.Left]
		} else {
			node = t.Nodes[node.Right]
		}
	}
	return node.Value
}

func parseFloat(s string) float64 {
	var v float64
	var neg bool
	var afterDot bool
	div := 1.0

	for _, c := range s {
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
				v += d / div
			} else {
				v = v*10 + d
			}
		}
	}
	if neg {
		return -v
	}
	return v
}
