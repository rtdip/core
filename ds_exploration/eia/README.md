## EIA Dataset Overview

The U.S. Energy Information Administration (EIA) provides a comprehensive set of open datasets covering all major aspects of the global energy system.  
These datasets are available through the **EIA Open Data API**, which organizes data into high-level categories and numerous subroutes.

### Query Builder

`scripts/eia_query_builder.py` contains a query builder to request datasets via the EIA API. Since the API only supplies 5000 rows / API call it might be tedious to do this by hand. The script takes a generated [API URL](https://www.eia.gov/opendata/browser) and polls it year by year.

*Note: You need to create an account and provide you own API key to query new data!*

*Also Note: There already implemented unit / integration tests for the query builder.*

### Main Categories (EIA API Root Endpoints)

| Main Category | Description |
|----------------|-------------|
| **Electricity** | Covers generation, consumption, transmission networks, and capacity statistics. |
| **Natural Gas** | Includes production, consumption, storage, and pricing data for natural gas. |
| **Petroleum** | Provides detailed data on refining, product output, inventories, and utilization. |
| **Crude Oil Imports** | Tracks imported crude volumes by origin, port, and transport method. |
| **Coal** | Reports production, exports, prices, and stock levels for coal and derivatives. |
| **Densified Biomass** | Contains data on pellet and biomass production and storage. |
| **Nuclear Plant Outages** | Lists generation outages, reactor capacity, and downtime. |
| **Outlook & Projections** | Provides energy market forecasts, fuel price projections, and CO₂ demand outlooks. |
| **Total Energy** | Aggregated statistics on total energy production and consumption across all sectors. |
| **State Energy Data System (SEDS)** | State-level energy production, consumption, and emissions breakdown. |
| **CO₂ Emissions** | Tracks emissions by source type, energy carrier, and sector. |
| **International Energy** | Global datasets on production, trade flows, and consumption by country. |

### Context for Shell-Related Analysis

Relevant main EIA categories for Shell refinery and energy forecasting:

- Petroleum  
- Crude Oil Imports  
- Natural Gas  
- Electricity  
- CO₂ Emissions  
- Total Energy

### EDA

Since the whole dataset is far too big for exploration the following subset is analyzed:

#### Rafinery Yield

*Dataset Structure*

| # | Column              | Non-Null Count | Dtype  |
|--:|----------------------|---------------:|:-------|
| 0 | period              | 120 186 | object |
| 1 | duoarea             | 120 186 | object |
| 2 | area-name           | 120 186 | object |
| 3 | product             | 120 186 | object |
| 4 | product-name        | 120 186 | object |
| 5 | process             | 120 186 | object |
| 6 | process-name        | 120 186 | object |
| 7 | series              | 120 186 | object |
| 8 | series-description  | 120 186 | object |
| 9 | value               | 116 668 | object |
|10 | units               | 120 186 | object |

**Total columns:** 11  
**Dtypes:** all `object`

*Dataset Overview*

| Column              | Count  | Unique | Top                                      | Freq  |
|----------------------|-------:|-------:|------------------------------------------|------:|
| period               | 120 186 | 229   | 2010-03                                 | 1 408 |
| duoarea              | 120 186 | 16    | NUS                                     | 9 457 |
| area-name            | 120 186 | 7     | NA                                      | 70 551 |
| product              | 120 186 | 56    | EPJKC                                   | 6 527 |
| product-name         | 120 186 | 56    | Commercial Kerosene-Type Jet Fuel       | 6 527 |
| process              | 120 186 | 1     | YPY                                     | 120 186 |
| process-name         | 120 186 | 1     | Refinery Net Production                 | 120 186 |
| series               | 120 186 | 1 575 | M_EPJKM_YPY_R50_MBBL                    | 227 |
| series-description   | 120 186 | 1 575 | West Coast (PADD 5) Refinery Net Production of… | 227 |
| value                | 116 668 | 13 794 | 0                                       | 5 951 |
| units                | 120 186 | 2     | MBBL                                    | 60 909 |

*Missing Values:*

| Column       | Missing Count | Missing % |
|---------------|---------------:|-----------:|
| value         | 3 518          | 2.93 %     |

Not every month from query is in the actual dataset `1992 - 2025`:

| Metric                | Value |
|------------------------|-------:|
| Unique periods (months) | 229 |

*Example plots*

![alt text](data/img/r3b_epjkc.png)

*Data Cleanup is needed to asses missing values when using not aggregated data!*

*Seasonality:*

![alt text](data/img/monthly_full.png)

| duoarea | trend_strength | seasonal_strength | n_points |
|----------|----------------|------------------|-----------|
| NUS      | 0.886752       | 0.933713         | 228       |
| R30      | 0.856845       | 0.902187         | 229       |
| R3B      | 0.856639       | 0.913179         | 228       |

![alt text](data/img/monthly_2006.png)

| duoarea | trend_strength | seasonal_strength | n_points |
|----------|----------------|------------------|-----------|
| NUS      | 0.410395       | 0.796275         | 80        |
| R30      | 0.342718       | 0.762639         | 80        |
| R3B      | 0.224469       | 0.765235         | 80        |

- **Before 2006:**  
  - Significantly higher *trend_strength* and *seasonal_strength* values (~0.85–0.9).  
  - Likely **distorted by outliers or structural changes** (e.g., unit, measurement, or reporting adjustments).  
  - Metrics **do not represent actual time-series behavior** but reflect historical inconsistencies.  

- **After 2006:**  
  - Values decrease to more realistic levels (*trend_strength* ≈ 0.2–0.4, *seasonal_strength* ≈ 0.75–0.8).  
  - Indicates **stable, consistent annual cycles** with limited long-term drift.  
  - Data are **more homogeneous and suitable for forecasting**.  

**Conclusion:**  
Historical data can provide broader coverage and longer time horizons, but **older records may distort time-series characteristics** due to structural changes, inconsistent measurement, or reporting differences.  
Such effects need to be **evaluated carefully** (e.g., variance or outlier analysis), and the **relevant modeling period should be selected based on data stability** rather than maximum historical length.

### Lookout

Further evaluation of **different data subsets** is required to verify whether similar issues (e.g., structural shifts, outliers, or inconsistent reporting) occur in other parts of the dataset.  
Each subset should be **assessed individually** to ensure data stability and reliability before inclusion in forecasting models.
