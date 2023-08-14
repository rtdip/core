import os
import pytest
from unittest.mock import Mock
from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.nc_raw_forecast_to_weather_data_model import (
    ExtractBase,
    ExtractPoint,
    ExtractGrid,
    WeatherForecastECMWFV1SourceTransformer,
) 

# Mocked ExtractBase for testing ExtractPoint and ExtractGrid
class MockExtractBase(ExtractBase):
    def raw(self, variables: list, method: str = "nearest"):
        return Mock()

def test_convert_ws_tag_names():
    tags = ["10u", "100u", "200u", "10v", "100v", "200v", "other"]
    converted_tags = ExtractBase.convert_ws_tag_names(tags)
    assert converted_tags == ["u10", "u100", "u200", "v10", "v100", "v200", "other"]

def test_extract_point():
    lat = 51.5
    lon = -0.1
    extractor = ExtractPoint(
        lat=lat,
        lon=lon,
        load_path="/path",
        date_start="2023-01-01",
        date_end="2023-01-02",
        run_interval="12",
        run_frequency="H",
        utc=True,
    )
    assert extractor.lat.values == lat
    assert extractor.lon.values == lon

def test_extract_grid():
    lat_min, lat_max, lon_min, lon_max, grid_step = 50.0, 52.0, -1.0, 1.0, 0.1
    extractor = ExtractGrid(
        lat_min=lat_min,
        lat_max=lat_max,
        lon_min=lon_min,
        lon_max=lon_max,
        grid_step=grid_step,
        load_path="/path",
        date_start="2023-01-01",
        date_end="2023-01-02",
        run_interval="12",
        run_frequency="H",
        utc=True,
    )
    assert extractor.lat_min == lat_min
    assert extractor.lat_max == lat_max
    assert extractor.lon_min == lon_min
    assert extractor.lon_max == lon_max

def test_transform_point():
    # Mock SparkSession for testing WeatherForecastECMWFV1SourceTransformer
    class MockSparkSession:
        pass

    lat = 51.5
    lon = -0.1
    extractor = MockExtractBase()
    transformer = WeatherForecastECMWFV1SourceTransformer(
        load_path="/path",
        date_start="2023-01-01",
        date_end="2023-01-02",
        lat=lat,
        lon=lon,
        lat_min=lat,
        lon_min=lon,
        lat_max=lat,
        lon_max=lon,
        variable_list=["var1", "var2"],
        TAG_PREFIX="prefix",
        Source="ECMWF",
    )
    transformer.extractor = extractor
    result = transformer.nc_transform_point()
    assert result is not None

def test_transform_grid():
    # Mock SparkSession for testing WeatherForecastECMWFV1SourceTransformer
    class MockSparkSession:
        pass

    extractor = MockExtractBase()
    transformer = WeatherForecastECMWFV1SourceTransformer(
        load_path="/path",
        date_start="2023-01-01",
        date_end="2023-01-02",
        lat=51.5,
        lon=-0.1,
        lat_min=50.0,
        lon_min=-1.0,
        lat_max=52.0,
        lon_max=1.0,
        variable_list=["var1", "var2"],
        TAG_PREFIX="prefix",
        Source="ECMWF",
    )
    transformer.extractor = extractor
    result = transformer.nc_transform_grid()
    assert result is not None

# Add more test functions as needed

if __name__ == "__main__":
    pytest.main()
