import pytest
import os
import pandas as pd
import numpy as np
import xarray as xr
from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.weather.ECMWF.nc_ExtractGrid_to_weather_data_model import ECMWFExtractGridToWeatherDataModel

# Sample test data
lat_max=54.9
lat_min=54.6
lon_max=6.9
lon_min=6.6
grid_step=0.1
load_path="../data/ecmwf/oper/fc/sfc/europe/"
date_start="2020-10-01 00:00:00"
date_end="2020-10-02 12:00:00"
run_interval="12"
run_frequency="H"

@pytest.fixture
def extract_instance():
    return ECMWFExtractGridToWeatherDataModel(
        lat_min, lat_max, lon_min, lon_max, grid_step,
        load_path, date_start, date_end, run_interval, run_frequency
    )

def test_constructor(extract_instance):
    assert extract_instance.lat_min == lat_min
    assert extract_instance.lat_max == lat_max
    assert extract_instance.lon_min == lon_min
    assert extract_instance.lon_max == lon_max
    assert extract_instance.grid_step == grid_step
    assert (extract_instance.lat == xr.DataArray(np.linspace(lat_min, lat_max, int(np.round((lat_max - lat_min) / grid_step)) + 1), dims=["latitude"])).all()
    assert (extract_instance.lon == xr.DataArray(np.linspace(lon_min, lon_max, int(np.round((lon_max - lon_min) / grid_step)) + 1), dims=["longitude"])).all()

def test_transform(extract_instance, mocker):
    # Mock xr.open_dataset to simulate opening a dataset
    class MockXROpenDataset:
        def __init__(self, data_vars):
            self.data_vars = data_vars
        def to_dataframe(self):
            return pd.DataFrame(self.data_vars)
        def close(self):
            pass
    mocker.patch('xarray.open_dataset', MockXROpenDataset)

    expected_df = pd.DataFrame([[1, 'a'], [2, 'b']], columns=["col1", "col2"])

    
    tag_prefix = "test_tag_prefix"
    variables = ["10u", "10v"]
    method = "nearest"
    df = extract_instance.transform(tag_prefix, variables, method)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == len(extract_instance.dates) * len(extract_instance.lat) * len(extract_instance.lon) * len(variables)
    assert all(col in df.columns for col in ["TagName", "Latitude", "Longitude", "EnqueuedTime", "EventTime", "EventDate", "Value", "Source", "Status", "Latest"])
    assert df["Source"] == "ECMWF_MARS"
    assert df["Status"] == "Good"
    assert df["Latest"] == True

