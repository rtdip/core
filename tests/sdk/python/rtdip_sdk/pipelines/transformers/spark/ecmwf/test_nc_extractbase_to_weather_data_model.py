import pytest
import os
import pandas as pd
import numpy as np
import xarray as xr
from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.ecmwf.nc_extractbase_to_weather_data_model import (
    ECMWFExtractBaseToWeatherDataModel,
)


# Sample test data
load_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "test_file")
date_start = "2021-01-01 00:00:00"
date_end = "2021-01-01 12:00:00"
run_interval = "12"
run_frequency = "H"
lat = xr.DataArray([10, 20], dims="latitude")
lon = xr.DataArray([30, 40], dims="longitude")


def test_convert_ws_tag_names():
    extract = ECMWFExtractBaseToWeatherDataModel(
        load_path, date_start, date_end, run_interval, run_frequency, lat, lon
    )
    tag_names = ["10u", "100u", "1000u", "10v"]
    converted_tags = extract._convert_ws_tag_names(tag_names)
    assert converted_tags == ["u10", "u100", "1000u", "v10"]


def test_transform():
    extract = ECMWFExtractBaseToWeatherDataModel(
        load_path, date_start, date_end, run_interval, run_frequency, lat, lon
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
