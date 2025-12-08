# Forecasting Visualization 

## Overview

visualization system provides two backends for creating forecast visualizations:

| **Matplotlib** | Static PNg 
| **Plotly** | Interactive HTML 

## Quick Start

### Option 1: Use in Pipeline 

```bash
cd amos_team_resources/shell

# Matplotlib (default) - Static PNG visualizations
python pipeline_shell_data.py

# Plotly - Interactive HTML visualizations
python pipeline_shell_data.py --use-plotly

# Regenerate only visualizations (skip training)
python pipeline_shell_data.py --skip-training --use-plotly
```

### Option 2: Use Standalone

```bash
cd amos_team_resources/visualization

# Matplotlib - Create config and run
python visualize_forecasts.py --config my_config.json

# Plotly 
python example_plotly_usage.py
```

## Usage Methods

### Pipeline Integration 

```bash
# Full pipeline with Matplotlib visualization
python pipeline_shell_data.py

# Full pipeline with Plotly visualization
python pipeline_shell_data.py --use-plotly

# Skip training, regenerate visualizations only
python pipeline_shell_data.py --skip-training --use-plotly

# Custom output directory
python pipeline_shell_data.py --visualization-output my_viz --use-plotly

# Skip visualization entirely
python pipeline_shell_data.py --skip-visualization
```

### Standalone Matplotlib 

#### Create Configuration File

```json
{
    "paths": {
        "predictions": "path/to/predictions.parquet",
        "historical": "path/to/historical.parquet",
        "actuals": "path/to/actuals.parquet",
        "output": "output_directory"
    },
    "columns": {
        "predictions": {
            "timestamp": "timestamp",
            "sensor_id": "item_id",
            "mean": "mean",
            "quantile_10": "0.1",
            "quantile_20": "0.2",
            "quantile_80": "0.8",
            "quantile_90": "0.9"
        },
        "historical": {
            "timestamp": "EventTime",
            "sensor_id": "TagName",
            "value": "Value"
        },
        "actuals": {
            "timestamp": "timestamp",
            "sensor_id": "item_id",
            "value": "target"
        }
    },
    "settings": {
        "lookback_hours": 168,
        "max_sensors_overview": 9,
        "detailed_sensors": 3,
        "datetime_format": "mixed"
    }
}
```

#### Run Visualization

```bash
python visualize_forecasts.py --config my_config.json
```

---

### Standalone Plotly 

```python
import pandas as pd
from visualization import forecasting_plotly

# Load data
predictions_df = pd.read_parquet("predictions.parquet")
historical_df = pd.read_parquet("historical.parquet")

# specific sensor
sensor_id = "SENSOR"
sensor_predictions = predictions_df[predictions_df['item_id'] == sensor_id]
sensor_historical = historical_df[historical_df['TagName'] == sensor_id]

# Standardize column names
forecast_data = pd.DataFrame({
    'timestamp': pd.to_datetime(sensor_predictions['timestamp']),
    'mean': sensor_predictions['mean'],
    'lower_60': sensor_predictions['0.2'],
    'upper_60': sensor_predictions['0.8'],
    'lower_80': sensor_predictions['0.1'],
    'upper_80': sensor_predictions['0.9']
})

historical_data = pd.DataFrame({
    'timestamp': pd.to_datetime(sensor_historical['EventTime']),
    'value': sensor_historical['Value']
})

# Create interactive plot
fig = forecasting_plotly.plot_forecast_with_confidence(
    historical_data=historical_data,
    forecast_data=forecast_data,
    forecast_start=forecast_data['timestamp'].min(),
    sensor_id=sensor_id,
    ci_levels=[60, 80]
)

# Save to HTML
forecasting_plotly.save_plotly_figure(fig, 'my_forecast.html', format='html')
```

---

## Configuration Guide

### Column Mapping Configuration

#### Example: Prophet Format

```python
column_mapping = {
    "predictions": {
        "timestamp": "ds",             
        "sensor_id": "sensor_name",
        "mean": "yhat",                
        "quantile_10": "yhat_lower",   
        "quantile_20": "yhat_lower",
        "quantile_80": "yhat_upper",   
        "quantile_90": "yhat_upper"
    },
    "historical": {
        "timestamp": "ds",
        "sensor_id": "sensor_name",
        "value": "y"                   
    },
    "actuals": {
        "timestamp": "ds",
        "sensor_id": "sensor_name",
        "value": "y"
    }
}
```

#### Example: LSTM/XGBoost Format

```python
column_mapping = {
    "predictions": {
        "timestamp": "date",
        "sensor_id": "sensor_id",
        "mean": "forecast",
        "quantile_10": "lower_bound",  
        "quantile_20": "lower_bound",
        "quantile_80": "upper_bound",  
        "quantile_90": "upper_bound"
    },
    "historical": {
        "timestamp": "date",
        "sensor_id": "sensor_id",
        "value": "value"
    },
    "actuals": {
        "timestamp": "date",
        "sensor_id": "sensor_id",
        "value": "actual"
    }
}
```

#### Example: No CI

```python
column_mapping = {
    "predictions": {
        "timestamp": "date",
        "sensor_id": "sensor",
        "mean": "forecast",
        "quantile_10": None,           
        "quantile_20": None,
        "quantile_80": None,
        "quantile_90": None
    },
    "historical": {
        "timestamp": "date",
        "sensor_id": "sensor",
        "value": "value"
    },
    "actuals": {
        "timestamp": "date",
        "sensor_id": "sensor",
        "value": "actual"
    }
}
```
---