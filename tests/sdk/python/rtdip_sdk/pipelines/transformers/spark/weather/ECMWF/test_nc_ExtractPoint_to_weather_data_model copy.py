import pytest
import os
import pandas as pd
import numpy as np
import xarray as xr
from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.weather.ECMWF.nc_ExtractPoint_to_weather_data_model import ECMWFExtractPointToWeatherDataModel

# Sample test data
tag_prefix = "point_"
lat = 10.0
lon = 20.0
load_path = "/path/to/load"
date_start = "2023-08-01 00:00:00"
date_end = "2023-08-02 00:00:00"
run_interval = "H"
run_frequency = "6H"

@pytest.fixture
def extract_instance():
    return ECMWFExtractPointToWeatherDataModel(
        tag_prefix, lat, lon, load_path, date_start, date_end, run_interval, run_frequency
    )

def test_constructor(extract_instance):
    assert extract_instance.tag_prefix == tag_prefix
    assert (extract_instance.lat == xr.DataArray([lat], dims=["latitude"])).all()
    assert (extract_instance.lon == xr.DataArray([lon], dims=["longitude"])).all()

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
    
    variables = ["10u", "10v"]
    method = "nearest"
    df = extract_instance.transform(variables, method)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == len(extract_instance.dates) * len(variables)
    assert all(col in df.columns for col in ["Latitude", "Longitude", "EnqueuedTime", "EventTime", "Value"])
    assert "Source" in df["Source"].unique()
    assert "Status" in df["Status"].unique()
    assert "Latest" in df["Latest"].unique()
    assert "EventDate" in df["EventDate"].unique()
    assert "TagName" in df["TagName"].unique()
