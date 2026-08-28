package scoring

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// ─── Scaler (Python → Go bridge) ─────────────────────────────────────────────

// ScalerParams holds StandardScaler parameters exported from Python training.
// Applied as: scaled = (raw - mean) / std
type ScalerParams struct {
	Version      string    `json:"version"`
	FeatureNames []string  `json:"feature_names"`
	Mean         []float64 `json:"mean"`
	Std          []float64 `json:"std"`
	Var          []float64 `json:"var"`
	NFeatures    int       `json:"n_features"`
}

// ─── XGBoost Native JSON Format ──────────────────────────────────────────────
//
// XGBoost exports models as JSON with this structure:
//
//	{
//	  "learner": {
//	    "learner_model_param": { "base_score": "0.5", "num_feature": "30" },
//	    "gradient_booster": {
//	      "model": {
//	        "trees": [
//	          {
//	            "split_indices":    [1, 5, 18, ...],   // feature index per node
//	            "split_conditions": [0.21, 0.94, ...],  // threshold per node
//	            "left_children":    [1, 3, 5, ...],     // left child index (-1 = leaf)
//	            "right_children":   [2, 4, 6, ...],     // right child index (-1 = leaf)
//	            "base_weights":     [0.0, -1.97, ...],  // leaf value (only for leaf nodes)
//	            "tree_param":       { "num_nodes": "19" }
//	          },
//	          ...
//	        ]
//	      }
//	    }
//	  }
//	}
//
// Each tree uses PARALLEL ARRAYS (not nested objects). Node i has:
//   - split_indices[i]    = feature index to split on
//   - split_conditions[i] = threshold value
//   - left_children[i]    = index of left child (-1 if leaf)
//   - right_children[i]   = index of right child (-1 if leaf)
//   - base_weights[i]     = output value (meaningful only for leaf nodes)

// xgboostModel represents the full XGBoost JSON structure.
type xgboostModel struct {
	Learner xgboostLearner `json:"learner"`
}

type xgboostLearner struct {
	ModelParam    xgboostModelParam    `json:"learner_model_param"`
	GradientBoost xgboostGradientBoost `json:"gradient_booster"`
}

type xgboostModelParam struct {
	BaseScore  string `json:"base_score"`
	NumFeature string `json:"num_feature"`
	NumClass   string `json:"num_class"`
}

type xgboostGradientBoost struct {
	Model xgboostGBModel `json:"model"`
}

type xgboostGBModel struct {
	Trees []xgboostTree `json:"trees"`
}

// xgboostTree is the raw XGBoost tree with parallel arrays.
type xgboostTree struct {
	SplitIndices    []int     `json:"split_indices"`
	SplitConditions []float64 `json:"split_conditions"`
	LeftChildren    []int     `json:"left_children"`
	RightChildren   []int     `json:"right_children"`
	BaseWeights     []float64 `json:"base_weights"`
	TreeParam       struct {
		NumNodes string `json:"num_nodes"`
	} `json:"tree_param"`
}

// ─── Compiled Model (optimized for inference) ────────────────────────────────

// compiledNode is a single node in the compiled decision tree.
type compiledNode struct {
	FeatureIdx int
	Threshold  float64
	Left       int // index into nodes array, -1 = leaf
	Right      int // index into nodes array, -1 = leaf
	Value      float64
	IsLeaf     bool
}

// compiledTree is a single decision tree compiled for fast inference.
type compiledTree struct {
	Nodes []compiledNode
}

// Predict walks the tree from root to leaf, returning the leaf value.
func (t *compiledTree) Predict(features []float64) float64 {
	if len(t.Nodes) == 0 {
		return 0
	}
	node := &t.Nodes[0]
	for !node.IsLeaf {
		if features[node.FeatureIdx] <= node.Threshold {
			node = &t.Nodes[node.Left]
		} else {
			node = &t.Nodes[node.Right]
		}
	}
	return node.Value
}

// Model wraps an XGBoost model loaded from the native JSON format.
// This is the production-ready model that parses real XGBoost exports.
type Model struct {
	Version    string
	LoadedAt   time.Time
	Thresholds Thresholds
	features   []string
	trees      []compiledTree
	baseScore  float64 // sigmoid(baseScore) = initial prediction (usually 0.5)
	numTrees   int
	scaler     *ScalerParams
	featureImportance map[string]float64
	mu         sync.RWMutex
}

type Thresholds struct {
	Approve float64
	Review  float64
	Decline float64
}

// Default feature names (same order as training pipeline).
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

// ─── Model Loading ───────────────────────────────────────────────────────────

// NewModel creates a fallback model with hardcoded rules.
// Used when no trained model file is available.
func NewModel() *Model {
	return &Model{
		Version:    "v1.0.0",
		LoadedAt:   time.Now(),
		Thresholds: Thresholds{Approve: 0.30, Review: 0.70, Decline: 1.00},
		features:   defaultFeatures,
		baseScore:  0.0,
		numTrees:   0,
		featureImportance: map[string]float64{
			"velocity_tx_count_1h":         0.15,
			"device_is_known":              0.12,
			"behavioral_amount_zscore":     0.11,
			"device_ip_country_match":      0.10,
			"velocity_unique_countries_1h": 0.09,
		},
	}
}

// LoadModel loads a trained XGBoost model from its native JSON export format.
//
// The JSON structure is:
//
//	{ "learner": { "gradient_booster": { "model": { "trees": [...] } } } }
//
// Each tree uses parallel arrays for nodes (split_indices, split_conditions,
// left_children, right_children, base_weights).
func LoadModel(path string) (*Model, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read model file: %w", err)
	}

	var xgbModel xgboostModel
	if err := json.Unmarshal(data, &xgbModel); err != nil {
		return nil, fmt.Errorf("failed to parse XGBoost JSON: %w", err)
	}

	// Parse base_score (string like "5E-1" → float64)
	baseScore := 0.5 // default
	if bs := xgbModel.Learner.ModelParam.BaseScore; bs != "" {
		if v, err := strconv.ParseFloat(bs, 64); err == nil {
			baseScore = v
		}
	}

	// Compile trees from parallel arrays to pointer-based trees
	rawTrees := xgbModel.Learner.GradientBoost.Model.Trees
	compiled := make([]compiledTree, len(rawTrees))
	totalNodes := 0

	for i, raw := range rawTrees {
		n := len(raw.SplitIndices)
		nodes := make([]compiledNode, n)
		for j := 0; j < n; j++ {
			left := raw.LeftChildren[j]
			right := raw.RightChildren[j]
			isLeaf := left == -1 && right == -1

			nodes[j] = compiledNode{
				FeatureIdx: raw.SplitIndices[j],
				Threshold:  raw.SplitConditions[j],
				Left:       left,
				Right:      right,
				Value:      raw.BaseWeights[j],
				IsLeaf:     isLeaf,
			}
		}
		compiled[i] = compiledTree{Nodes: nodes}
		totalNodes += n
	}

	m := &Model{
		Version:    "v1.0.0",
		LoadedAt:   time.Now(),
		Thresholds: Thresholds{Approve: 0.30, Review: 0.70, Decline: 1.00},
		features:   defaultFeatures,
		trees:      compiled,
		baseScore:  baseScore,
		numTrees:   len(compiled),
	}

	log.Info().
		Int("num_trees", len(compiled)).
		Int("total_nodes", totalNodes).
		Float64("base_score", baseScore).
		Msg("XGBoost model loaded successfully")

	return m, nil
}

// ─── Scaler Loading ──────────────────────────────────────────────────────────

// LoadScaler loads StandardScaler parameters from the JSON exported by Python.
func LoadScaler(path string) (*ScalerParams, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read scaler file: %w", err)
	}

	var scaler ScalerParams
	if err := json.Unmarshal(data, &scaler); err != nil {
		return nil, fmt.Errorf("failed to parse scaler file: %w", err)
	}

	if len(scaler.Mean) != len(scaler.Std) {
		return nil, fmt.Errorf("scaler mean/std length mismatch: %d vs %d", len(scaler.Mean), len(scaler.Std))
	}

	log.Info().
		Str("version", scaler.Version).
		Int("n_features", scaler.NFeatures).
		Msg("Scaler loaded successfully")

	return &scaler, nil
}

// SetScaler attaches a loaded ScalerParams to the Model.
func (m *Model) SetScaler(scaler *ScalerParams) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.scaler = scaler
	log.Info().Str("version", scaler.Version).Msg("Scaler attached to model")
}

// NumTrees returns the number of trees in the ensemble.
func (m *Model) NumTrees() int {
	return m.numTrees
}

// BaseScore returns the base score (initial prediction before trees).
func (m *Model) BaseScore() float64 {
	return m.baseScore
}

// ─── Prediction ──────────────────────────────────────────────────────────────
//
// The inference pipeline is:
//
//	1. Extract raw features from map[string]string → []float64
//	2. Apply StandardScaler: (x - μ) / σ
//	3. Sum tree predictions: logit = Σ tree_i.predict(scaled_features)
//	4. Add base_score: logit += base_score
//	5. Apply sigmoid: probability = 1 / (1 + exp(-logit))
//
// This matches exactly what XGBoost does internally:
//
//	predict = base_score + Σ tree_i(features)
//	probability = sigmoid(predict)

// Predict computes fraud probability from a feature vector.
// Returns probability [0, 1] and top feature contributions for explainability.
func (m *Model) Predict(features map[string]string) (float64, map[string]float64) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Step 1: Extract raw feature values
	rawFeatures := m.extractFeatures(features)

	// Step 2: Apply StandardScaler normalization (matches Python training)
	scaledFeatures := m.scaleFeatures(rawFeatures)

	// Step 3: Sum predictions from all trees
	logit := m.baseScore
	for i := range m.trees {
		logit += m.trees[i].Predict(scaledFeatures)
	}

	// Step 4: Apply sigmoid to get probability
	probability := sigmoid(logit)
	probability = math.Max(0.0, math.Min(1.0, probability))

	// Step 5: Compute feature contributions for explainability
	topFeatures := m.computeFeatureContributions(rawFeatures)

	return probability, topFeatures
}

// PredictRaw returns the raw logit before sigmoid (for debugging).
func (m *Model) PredictRaw(features map[string]string) float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	rawFeatures := m.extractFeatures(features)
	scaledFeatures := m.scaleFeatures(rawFeatures)

	logit := m.baseScore
	for i := range m.trees {
		logit += m.trees[i].Predict(scaledFeatures)
	}
	return logit
}

// ─── Feature Processing ──────────────────────────────────────────────────────

// extractFeatures converts the string-valued feature map to a float64 array.
func (m *Model) extractFeatures(features map[string]string) []float64 {
	vals := make([]float64, len(m.features))
	for i, name := range m.features {
		if v, ok := features[name]; ok {
			vals[i] = parseFloat(v)
		}
	}
	return vals
}

// scaleFeatures applies StandardScaler: (x - mean) / std
func (m *Model) scaleFeatures(raw []float64) []float64 {
	if m.scaler == nil {
		return raw
	}
	scaled := make([]float64, len(raw))
	for i, v := range raw {
		std := m.scaler.Std[i]
		if std > 1e-10 {
			scaled[i] = (v - m.scaler.Mean[i]) / std
		} else {
			scaled[i] = 0.0
		}
	}
	return scaled
}

// computeFeatureContributions estimates each feature's contribution to the score.
func (m *Model) computeFeatureContributions(features []float64) map[string]float64 {
	contributions := make(map[string]float64)
	// Simple contribution: importance × scaled feature value
	// In production, use SHAP values via native XGBoost bindings
	for name, importance := range m.featureImportance {
		for i, fname := range m.features {
			if fname == name {
				contributions[name] = importance * features[i]
				break
			}
		}
	}
	return contributions
}

// GetTopFeatures returns the top N features by absolute contribution.
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

// ─── Utilities ───────────────────────────────────────────────────────────────

func sigmoid(x float64) float64 {
	return 1.0 / (1.0 + math.Exp(-x))
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
