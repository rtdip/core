import pytest
import os
import pandas as pd
import numpy as np
import xarray as xr
from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.ecmwf.nc_extractpoint_to_weather_data_model import (
    ECMWFExtractPointToWeatherDataModel,
)

# Sample test data
lat = 55.7
lon = 6.6
load_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "test_file")
date_start = "2021-01-01 00:00:00"
date_end = "2021-01-01 12:00:00"
run_interval = "12"
run_frequency = "H"


def test_constructor():
    extract = ECMWFExtractPointToWeatherDataModel(
        lat, lon, load_path, date_start, date_end, run_interval, run_frequency
    )
    assert (extract.lat == xr.DataArray([lat], dims=["latitude"])).all()
    assert (extract.lon == xr.DataArray([lon], dims=["longitude"])).all()


def test_transform():
    extract = ECMWFExtractPointToWeatherDataModel(
        lat, lon, load_path, date_start, date_end, run_interval, run_frequency
    )

    tag_prefix = "test_tag_prefix"
    variables = ["10u", "100u"]
    method = "nearest"
    df = extract.transform(tag_prefix, variables, method)

    assert isinstance(df, pd.DataFrame)
    assert all(
        col in df.columns
        for col in [
            "TagName",
            "Latitude",
            "Longitude",
            "EnqueuedTime",
            "EventTime",
            "EventDate",
            "Value",
            "Source",
            "Status",
            "Latest",
        ]
    )
    assert df["Latest"].all() == True
