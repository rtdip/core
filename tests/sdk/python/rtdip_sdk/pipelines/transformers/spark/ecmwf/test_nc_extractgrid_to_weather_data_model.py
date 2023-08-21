import pytest
import os
import pandas as pd
import numpy as np
import xarray as xr
from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.ecmwf.nc_extractgrid_to_weather_data_model import (
    ECMWFExtractGridToWeatherDataModel,
)

# Sample test data
lat_max = 54.9
lat_min = 54.6
lon_max = 6.9
lon_min = 6.6
grid_step = 0.1
load_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "test_file")
date_start = "2021-01-01 00:00:00"
date_end = "2021-01-01 12:00:00"
run_interval = "12"
run_frequency = "H"


def test_constructor():
    extract = ECMWFExtractGridToWeatherDataModel(
        lat_min,
        lat_max,
        lon_min,
        lon_max,
        grid_step,
        load_path,
        date_start,
        date_end,
        run_interval,
        run_frequency,
    )
    assert extract.lat_min == lat_min
    assert extract.lat_max == lat_max
    assert extract.lon_min == lon_min
    assert extract.lon_max == lon_max
    assert extract.grid_step == grid_step
    assert (
        extract.lat
        == xr.DataArray(
            np.linspace(
                lat_min, lat_max, int(np.round((lat_max - lat_min) / grid_step)) + 1
            ),
            dims=["latitude"],
        )
    ).all()
    assert (
        extract.lon
        == xr.DataArray(
            np.linspace(
                lon_min, lon_max, int(np.round((lon_max - lon_min) / grid_step)) + 1
            ),
            dims=["longitude"],
        )
    ).all()


def test_transform():
    extract = ECMWFExtractGridToWeatherDataModel(
        lat_min,
        lat_max,
        lon_min,
        lon_max,
        grid_step,
        load_path,
        date_start,
        date_end,
        run_interval,
        run_frequency,
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
