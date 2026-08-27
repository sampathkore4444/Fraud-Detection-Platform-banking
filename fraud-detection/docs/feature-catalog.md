# Fraud Detection Feature Catalog

## Overview

30 features across 3 groups (Velocity, Behavioral, Device) used for fraud scoring.

---

## Velocity Features (SPEC §3.2.2)

Windowed counters over short time horizons.

| # | Feature | Type | Window | Aggregation | Key | Description |
|---|---------|------|--------|-------------|-----|-------------|
| 1 | `velocity_tx_count_1h` | int | 1h tumbling | COUNT(*) | account_id | Transaction count in last 1 hour |
| 2 | `velocity_tx_count_24h` | int | 24h tumbling | COUNT(*) | account_id | Transaction count in last 24 hours |
| 3 | `velocity_amount_sum_1h` | float | 1h tumbling | SUM(amount) | account_id | Sum of amounts in last 1 hour |
| 4 | `velocity_amount_sum_24h` | float | 24h tumbling | SUM(amount) | account_id | Sum of amounts in last 24 hours |
| 5 | `velocity_decline_count_24h` | int | 24h tumbling | COUNT(*) WHERE declined | account_id | Declined transaction count in last 24h |
| 6 | `velocity_unique_countries_1h` | int | 1h session | COUNT(DISTINCT country) | account_id | Distinct countries in last 1 hour |
| 7 | `velocity_unique_merchants_24h` | int | 24h tumbling | COUNT(DISTINCT merchant) | account_id | Distinct merchants in last 24 hours |
| 8 | `velocity_avg_amount_7d` | float | 7d tumbling | AVG(amount) | account_id | Average amount over last 7 days |
| 9 | `velocity_stddev_amount_7d` | float | 7d tumbling | STDDEV(amount) | account_id | Stddev of amounts over last 7 days |
| 10 | `velocity_time_since_last_tx` | long | event time | LAST event time delta | account_id | Seconds since last transaction |

### Fraud Signals
- High `velocity_tx_count_1h` → burst detection
- High `velocity_unique_countries_1h` → impossible travel
- High `velocity_amount_sum_24h` → unusual spending
- Low `velocity_time_since_last_tx` → rapid-fire transactions

---

## Behavioral Features (SPEC §3.2.3)

Pattern-based features computed over longer horizons and user profiles.

| # | Feature | Type | Description | State Source |
|---|---------|------|-------------|--------------|
| 11 | `behavioral_typical_amount_ratio` | float | current_amount / avg_amount_7d | Flink state |
| 12 | `behavioral_typical_hour_score` | float | P(hour_of_day | account history) | Flink state |
| 13 | `behavioral_typical_day_score` | float | P(day_of_week | account history) | Flink state |
| 14 | `behavioral_merchant_category_diversity` | int | Unique MCC count in last 30d | Flink state |
| 15 | `behavioral_amount_zscore` | float | Z-score against account history | Flink state |
| 16 | `behavioral_is_recipient_new` | int | First-time recipient (0/1) | Flink state |
| 17 | `behavioral_velocity_direction` | float | Inflow vs outflow ratio 7d | Flink state |
| 18 | `behavioral_time_between_tx_stddev` | float | Stddev of inter-tx intervals | Flink state |
| 19 | `behavioral_country_change_freq` | float | Country changes per day over 30d | Flink state |
| 20 | `behavioral_night_tx_ratio` | float | Ratio of transactions between 23:00-05:00 | Flink state |

### Fraud Signals
- High `behavioral_typical_amount_ratio` → unusual amount for this user
- Low `behavioral_typical_hour_score` → transaction at unusual time
- High `behavioral_amount_zscore` → amount far from normal
- `behavioral_is_recipient_new` = 1 → new recipient risk
- High `behavioral_country_change_freq` → frequent country changes
- High `behavioral_night_tx_ratio` → mostly nighttime activity

---

## Device & Context Features (SPEC §3.2.4)

Device and context features computed from device fingerprints and IP data.

| # | Feature | Type | Description | State Source |
|---|---------|------|-------------|--------------|
| 21 | `device_is_known` | int | Device seen in last 90d (0/1) | Redis lookup |
| 22 | `device_last_seen_hours_ago` | float | Hours since device last used | Redis lookup |
| 23 | `device_unique_accounts_24h` | int | Distinct accounts on same device 24h | Flink state |
| 24 | `device_is_emulator_detected` | int | Known emulator fingerprint (0/1) | Redis lookup |
| 25 | `device_rooted_jailbroken` | int | Rooted/jailbroken signal (0/1) | Flink state |
| 26 | `device_ip_country_match` | int | IP geolocation country == tx country (0/1) | Real-time geo |
| 27 | `device_ip_is_vpn` | int | VPN/proxy detected (0/1) | Redis lookup |
| 28 | `device_browser_fingerprint_match` | int | Fingerprint matches known (0/1) | Redis lookup |
| 29 | `device_latency_anomaly` | int | API response time anomaly (0/1) | Flink state |
| 30 | `device_is_new_os_version` | int | OS version changed (0/1) | Flink state |

### Fraud Signals
- `device_is_known` = 0 → first-time device risk
- `device_is_emulator_detected` = 1 → emulator fraud
- `device_ip_country_match` = 0 → geo anomaly
- `device_ip_is_vpn` = 1 → VPN/proxy risk
- `device_unique_accounts_24h` > 5 → device farming
- `device_latency_anomaly` = 1 → potential bot activity

---

## Feature Importance (Expected)

Based on model analysis, top features by importance:

| Rank | Feature | Importance | Signal |
|------|---------|------------|--------|
| 1 | `velocity_tx_count_1h` | 0.15 | Burst detection |
| 2 | `device_is_known` | 0.12 | First-time device risk |
| 3 | `behavioral_amount_zscore` | 0.11 | Unusual amount |
| 4 | `device_ip_country_match` | 0.10 | Geo anomaly |
| 5 | `velocity_unique_countries_1h` | 0.09 | Impossible travel |

---

## Feature Validation Rules

Per SPEC §3.2.5, all features must pass validation before scoring:

| Feature | Min | Max | Type | Default |
|---------|-----|-----|------|---------|
| velocity_tx_count_1h | 0 | 1000 | int | 0 |
| velocity_tx_count_24h | 0 | 10000 | int | 0 |
| velocity_amount_sum_1h | 0 | 1000000 | float | 0.0 |
| velocity_amount_sum_24h | 0 | 10000000 | float | 0.0 |
| velocity_decline_count_24h | 0 | 1000 | int | 0 |
| velocity_unique_countries_1h | 0 | 50 | int | 0 |
| velocity_unique_merchants_24h | 0 | 500 | int | 0 |
| velocity_avg_amount_7d | 0 | 1000000 | float | 0.0 |
| velocity_stddev_amount_7d | 0 | 1000000 | float | 0.0 |
| velocity_time_since_last_tx | 0 | 31536000 | float | 0.0 |
| behavioral_typical_amount_ratio | 0 | 100 | float | 1.0 |
| behavioral_typical_hour_score | 0 | 1 | float | 0.0417 |
| behavioral_typical_day_score | 0 | 1 | float | 0.1429 |
| behavioral_merchant_category_diversity | 0 | 500 | int | 0 |
| behavioral_amount_zscore | -10 | 10 | float | 0.0 |
| behavioral_is_recipient_new | 0 | 1 | int | 0 |
| behavioral_velocity_direction | 0 | 100 | float | 1.0 |
| behavioral_time_between_tx_stddev | 0 | 86400 | float | 0.0 |
| behavioral_country_change_freq | 0 | 30 | float | 0.0 |
| behavioral_night_tx_ratio | 0 | 1 | float | 0.0 |
| device_is_known | 0 | 1 | int | 0 |
| device_last_seen_hours_ago | 0 | 999999 | float | 999999 |
| device_unique_accounts_24h | 0 | 100 | int | 0 |
| device_is_emulator_detected | 0 | 1 | int | 0 |
| device_rooted_jailbroken | 0 | 1 | int | 0 |
| device_ip_country_match | 0 | 1 | int | 0 |
| device_ip_is_vpn | 0 | 1 | int | 0 |
| device_browser_fingerprint_match | 0 | 1 | int | 0 |
| device_latency_anomaly | 0 | 1 | int | 0 |
| device_is_new_os_version | 0 | 1 | int | 0 |
