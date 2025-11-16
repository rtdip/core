## Exploratory Data Analysis (EDA) – SCADA 2020 Subset

### Goal
Pre-assessment of data quality, temporal structure, and forecast suitability of the SCADA 2020 turbine dataset.

### 1. Data Structure
- ~52 k records and 299 numerical or derived sensor features.  
- Consistent types: `float64` for sensor values, `object` only for timestamps.  

### 2. Missing Values

| Column | Missing Count | Missing % |
|----------------------------------------------|---------------:|-------------:|
| Potential power met mast anemometer MPC (kW) | 52 704 | 100.000000 |
| Lost Production (Contractual Global) (kWh)   | 52 704 | 100.000000 |
| Potential power met mast anemometer (kW)     | 52 704 | 100.000000 |
| Production-based Contractual Avail. (Global) | 52 704 | 100.000000 |
| Production-based Contractual Avail. (Custom) | 52 704 | 100.000000 |
| Time-based Contractual Avail. (Custom)       | 52 704 | 100.000000 |
| Equivalent Full Load Hours counter (s)       | 52 704 | 100.000000 |
| Lost Production (Contractual Custom) (kWh)   | 52 704 | 100.000000 |
| Time-based Contractual Avail. (Global)       | 52 704 | 100.000000 |
| Reactive Energy Export counter (kvarh)       | 52 631 | 99.861491 |
| Potential power estimated (kW)               | 52 236 | 99.112022 |
| Energy Import counter (kWh)                  | 50 668 | 96.136916 |
| Production-based IEC B.2.3 (Users View)       | 6 641 | 12.600562 |
| Lost Production (Production-based IEC B.2.3) (kWh) | 6 532 | 12.393746 |
| Energy Export counter (kWh)                   | 5 896 | 11.187007 |
| Reactive Energy Import counter (kvarh)        | 5 316 | 10.086521 |
| Reactive Energy Export (kvarh)                | 5 292 | 10.040984 |
| Drive train acceleration, Min (mm/ss)         | 4 396 | 8.340923 |
| Drive train acceleration, Max (mm/ss)         | 4 396 | 8.340923 |
| Drive train acceleration, StdDev (mm/ss)      | 4 396 | 8.340923 |

### 3. Temporal Consistency
- Regular sampling interval of ≈ 10 minutes.  
- No timestamp duplicates or large gaps detected.  
- Dataset represents a continuous operational period suitable for forecasting.

### 4. Outlier Analysis 
- Overall data integrity: **high**, no sensor failures visible.  
- Daily aggregation of zero-power periods revealed only three brief downtime peaks (June, July, October 2020), each below 0.7 % of total operating time → overall turbine availability remains high.  
- Anomaly screening identified 258 records where wind speed > 6 m/s but power < 100 kW → likely short curtailment or sensor irregularities rather than mechanical faults. 

![alt text](data/img/zero_share.png)

### 6. Automated Trend / Seasonality Index
| Metric | Interpretation |
|---------|----------------|
| `trend_strength ≈ 1` | High mean variation (often cumulative counters or strong seasonality) |
| `seasonal_strength ≈ 1` | Pronounced periodic behavior (daily / annual cycles) |
| `≈ 0` | Stationary or noisy signals |

→ `trend≈1, seasonality≈1` → cumulative, non-forecastable.  

These variables are cumulative meters that only ever increase over time.
Their steady growth produces a perfect trend, while recurring daily or weekly production cycles add a strong seasonal pattern.
Because the next value can be derived almost directly from the previous one (value(t+1) ≈ value(t) + Δ), they contain no new predictive information — they simply reflect the integrated sum of past power output, not a dynamic process to be forecast.

→ Physical signals like *Hub temperature (°C)* show strong seasonality → relevant for forecasting.

### 7. TS w/ Seasonality - Example: Power (kW)

| column     | trend_strength | seasonal_strength |
|-------------|----------------|-------------------|
| Power (kW)  | 0.680761       | 0.765009          |

![alt text](data/img/mean_power.png)


It is very likely that the following features show seasonality since `Power (kW)` is strongly correlated to them:

![alt text](data/img/power_corr.png)

### 7. Status 2020 Subset (Event Logs)
- 8 737 records, 8 columns (`Timestamp start`, `end`, `Duration`, `Status`, `Code`, `Message`, …).  
- `Comment` 100 % NaN 
- `Service contract category` ≈ 74 % NaN
- Core operational fields: complete and consistent → usable for downtime correlation.
