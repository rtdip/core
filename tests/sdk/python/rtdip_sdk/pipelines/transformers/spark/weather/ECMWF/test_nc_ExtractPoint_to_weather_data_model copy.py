import pytest
import os
import pandas as pd
import numpy as np
import xarray as xr
from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.weather.ECMWF.nc_ExtractPoint_to_weather_data_model import ECMWFExtractPointToWeatherDataModel

# Sample test data
tag_prefix = "test"
lat=55.7
lon=6.6
load_path="../data/ecmwf/oper/fc/sfc/europe/"
date_start="2020-10-01 00:00:00",
date_end="2020-10-02 12:00:00"
run_interval="12"
run_frequency="H"

@pytest.fixture
def extract_instance():
    return ECMWFExtractPointToWeatherDataModel(
        lat, lon, load_path, date_start, date_end, run_interval, run_frequency
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