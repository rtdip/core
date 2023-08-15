import pytest
import os
import pandas as pd
import numpy as np
import xarray as xr
from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.weather.ECMWF.nc_ExtractBase_to_weather_data_model import ExtractBase

# Sample test data
load_path = "/path/to/load"
date_start = "2023-08-01 00:00:00"
date_end = "2023-08-02 00:00:00"
run_interval = "H"
run_frequency = "12H"
lat = xr.DataArray([10, 20], dims="latitude")
lon = xr.DataArray([30, 40], dims="longitude")

@pytest.fixture
def extract_instance():
    return ExtractBase(
        load_path, date_start, date_end, run_interval, run_frequency, lat, lon
    )

def test_convert_ws_tag_names():
    extract = ExtractBase(load_path, date_start, date_end, run_interval, run_frequency, lat, lon)
    tag_names = ["10u", "100u", "1000u", "10v"]
    converted_tags = extract._convert_ws_tag_names(tag_names)
    assert converted_tags == ["u10", "u100", "1000u", "v10"]

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
    
    tag_prefix = "wind_"
    variables = ["10u", "10v"]
    method = "nearest"
    df = extract_instance.transform(tag_prefix, variables, method)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == len(extract_instance.dates) * len(lat) * len(lon) * len(variables)
    assert all(col in df.columns for col in ["Latitude", "Longitude", "EnqueuedTime", "EventTime", "Value"])
    assert "Source" in df["Source"].unique()
    assert "Status" in df["Status"].unique()
    assert "Latest" in df["Latest"].unique()
    assert "EventDate" in df["EventDate"].unique()
    assert "TagName" in df["TagName"].unique()