---
date: 2026-04-01
authors:
  - GBARAS
---

<center>
# Important: Module Removals and Deprecations in RTDIP SDK v0.14.3

<img src="/blog/images/framework.png" width="60%" alt="breaking-changes" />
</center>

We are pleased to announce significant updates to the RTDIP SDK that improve performance and focus the platform on its core strengths. These changes involve the removal of several modules and the deprecation of turbodbc support. This post outlines these changes and provides guidance on migration paths.

<!-- more -->

## Summary of Changes

### Removed Modules

The following modules have been completely removed from RTDIP SDK v0.14.4:

#### 1. Data Quality Module (`rtdip_sdk.pipelines.data_quality`)
The entire data quality module, including monitoring and data manipulation components, has been removed. This includes:

- **Monitoring Components:** CheckValueRanges, FlatlineDetection, IdentifyMissingDataInterval, IdentifyMissingDataPattern, MovingAverage, GreatExpectationsDataQuality
- **Data Manipulation Components:** DimensionalityReduction, DuplicateDetection, FlatlineFilter, GaussianSmoothing, IntervalFiltering, KSigmaAnomalyDetection, MissingValueImputation, OutOfRangeValueFilter
- **Normalization Components:** NormalizationZScore, NormalizationMean, NormalizationMinMax, Denormalization
- **Base Interfaces:** InputValidator, DataManipulationBaseInterface, MonitoringBaseInterface

**Impact:** Applications using these components will need to be refactored to use alternative solutions or upgraded to implement data quality checks independently.

#### 2. Forecasting Module (`rtdip_sdk.pipelines.forecasting`)
The entire forecasting module has been removed. This includes:

- **Time Series Forecasting:** ArimaPrediction, ArimaAutoPrediction
- **Machine Learning Models:** LinearRegression, KNearestNeighbors, DataBinning
- **Base Interface:** MachineLearningInterface

**Impact:** Applications using ARIMA, AutoARIMA, or other forecasting models will need to migrate to external forecasting libraries or implement their own models.

#### 3. Associated Tests
All test files for the removed modules have been removed from the test suite, including:
- 23 data quality tests
- 6 forecasting tests
- 1 logging integration test (`test_log_collection.py`)

### 📦 Removed Package Dependencies
The following packages, which were only used by the removed modules, have been removed from dependencies:

- `statsmodels>=0.14.1` - Used by ARIMA models
- `pmdarima>=2.0.4` - Used by AutoARIMA
- `great-expectations>=0.18.8` - Used by data quality monitoring
- `scikit-learn>=1.3.0` - Used by machine learning components

### Deprecated: Turbodbc Support

**Turbodbc is no longer inherently supported in RTDIP SDK (as of v0.14.4).** The connector is maintained for backward compatibility, but is deprecated.

#### Migration Options
If you require turbodbc connectivity, you have two choices:

**Option 1: Use an older RTDIP version**
```bash
pip install "rtdip-sdk<=0.14.3"
```

**Option 2: Manual Installation**
Install turbodbc v0.14.3 or earlier manually in your environment:
```bash
pip install "turbodbc"
```

#### Alternative Connectors
For new projects, we recommend using:
- **DatabricksSQLConnection** (Default and recommended)
- **PYODBCSQLConnection** (Lightweight ODBC alternative)
- **SparkConnection** (For Spark-based workloads)

## Affected Components & Migration Guidance

### If You Were Using Data Quality Monitoring
**Previous Approach:**
```python
from rtdip_sdk.pipelines.data_quality.monitoring.spark import IdentifyMissingDataInterval
```

**Migration:**
- Implement custom monitoring logic
- Use external data quality frameworks (e.g., Great Expectations directly)
- Implement monitoring in your data pipeline transforms

### If You Were Using Data Quality Data Manipulation
**Previous Approach:**
```python
from rtdip_sdk.pipelines.data_quality.data_manipulation.spark import MissingValueImputation
```

**Migration:**
- Use PySpark SQL and DataFrame operations directly
- Implement transform logic in pipeline steps
- Consider external libraries for specialized operations

### If You Were Using Forecasting
**Previous Approach:**
```python
from rtdip_sdk.pipelines.forecasting.spark import ArimaAutoPrediction
```

**Migration:**
- Use external forecasting libraries:
  - [statsmodels](https://www.statsmodels.org/) for ARIMA/AutoARIMA
  - [prophet](https://facebook.github.io/prophet/) for time series
  - [scikit-learn](https://scikit-learn.org/) for ML models
- Implement forecasting in separate pipeline steps or services

### If You Were Using Turbodbc
**Previous Approach:**
```python
from rtdip_sdk.connectors import TURBODBCSQLConnection
```

**Migration:**
```python
# Option 1: Use PYODBC instead (recommended)
from rtdip_sdk.connectors import PYODBCSQLConnection

# Option 2: Continue with turbodbc (manual installation)
# Install turbodbc
# No code changes needed, deprecation warnings will appear
```

## When to Update

We recommend updating to v0.14.4 if:
- You are not using data quality, forecasting, or turbodbc features
- You are ready to migrate to alternative solutions for removed functionality
- You want the latest RTDIP updates and improvements

**Delay updating if:**
- You heavily rely on data quality monitoring/manipulation
- You use ARIMA/forecasting extensively
- You require turbodbc connectivity

For delayed updates, continue using your current RTDIP version that includes these features, or implement them as external components.

## Documentation & Resources

For more information on the affected modules and alternatives:

- **Available Connectors:** [Connector Documentation](../../sdk/queries/connectors.md)
- **Authentication:** [Azure Authentication Guide](../../sdk/authentication/azure.md)
- **Installation:** [Getting Started](../../getting-started/installation.md)
- **Removed Components Documentation:** [DATA_QUALITY_REMOVAL.md](https://github.com/rtdip/core/blob/develop/DATA_QUALITY_REMOVAL.md)

## Support & Questions

If you have questions or concerns about these changes:

1. **Update your installation guide:** See [Installation Documentation](../../getting-started/installation.md) for the latest requirements
2. **Open an Issue:** Visit [GitHub Issues](https://github.com/rtdip/core/issues) to discuss migration paths
3. **Check Migration Guides:** Look for updated documentation on connector alternatives

## Looking Forward

These changes allow RTDIP to:
- Focus on core time-series data ingestion and querying capabilities
- Maintain lean, focused dependencies
- Provide better performance and maintainability
- Allow users to choose specialized tools for data quality and forecasting

We appreciate your understanding and are committed to supporting your migration journey.

---

**Questions?** 
- Open an issue on [GitHub](https://github.com/rtdip/core/issues)
- Check the [documentation](../../sdk/overview.md)
