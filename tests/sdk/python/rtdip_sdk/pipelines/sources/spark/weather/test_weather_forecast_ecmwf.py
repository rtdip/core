#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import pandas as pd
import numpy as np
import os

from pyspark.sql import SparkSession

from src.sdk.python.rtdip_sdk.pipelines.sources.spark.weather.weather_forecast_ecmwf import WeatherForecastECMWFSource


from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.weather import WEATHER_FORECAST_SCHEMA
from pytest_mock import MockerFixture

import unittest
import pytest
from unittest.mock import Mock, patch

# Mocked SparkSession for testing
class MockSparkSession:
    pass

# Mocked ECMWFService for MARSDownloader testing
class MockECMWFService:
    def execute(self, req_dict, target):
        pass


def test_retrieve():
    downloader = MARSDownloader(date_start="2023-01-01", date_end="2023-01-02", save_path="/path")
    with patch("Parallel") as mock_parallel, patch("ECMWFService", return_value=MockECMWFService()):
        mock_parallel.return_value = Mock(success=[1, 1])
        result = downloader.retrieve(mars_dict={}, n_jobs=2)
    assert result.success == [1, 1]
    assert result.retrieve_ran == True

def test_info():
    downloader = MARSDownloader(date_start="2023-01-01", date_end="2023-01-02", save_path="/path")
    downloader.retrieve_ran = True
    downloader.success = [1, 0]
    result = downloader.info()
    assert result.tolist() == [True, False]

def setUp(self):
        # Initialize test data and objects
        self.save_path = "/path/to/save"
        self.date_start = "2023-08-01 00:00:00"
        self.date_end = "2023-08-02 00:00:00"
        self.ecmwf_class = "your_ecmwf_class"
        self.stream = "your_stream"
        self.expver = "your_expver"
        self.leveltype = "your_leveltype"
        self.ec_vars = ["var1", "var2"]
        self.forecast_area = [40, -10, 35, -5]  # Replace with your specific coordinates
        self.weather_source = WeatherForecastECMWFV1Source(
            self.spark,
            None,  # You can pass the appropriate SparkContext here if needed
            self.save_path,
            self.date_start,
            self.date_end,
            self.ecmwf_class,
            self.stream,
            self.expver,
            self.leveltype,
            self.ec_vars,
            self.forecast_area
        )

def test_get_lead_time(self):
    lead_times = self.weather_source._get_lead_time()
    expected_lead_times = [0, 1, 2, ..., 243, 244, 245]
    self.assertEqual(lead_times, expected_lead_times)

def test_get_api_params(self):
    lead_times = [0, 1, 2, 3]
    params = self.weather_source._get_api_params(lead_times)
    expected_params = {
            "class": self.ecmwf_class,
            "stream": self.stream,
            "expver": self.expver,
            "levtype": self.leveltype,
            "type": "fc",
            "param": self.ec_vars,
            "step": lead_times,
            "area": self.forecast_area,
            "grid": [0.1, 0.1]
        }
    self.assertEqual(params, expected_params)
    
def test_weather_forecast_ecmwf_v1_source():
    spark = MockSparkSession()
    sc = spark.sparkContext
    weather_source = WeatherForecastECMWFV1Source(
        spark=spark,
        sc=sc,
        save_path="/path",
        date_start="2023-01-01",
        date_end="2023-01-02",
        ecmwf_class="class",
        stream="stream",
        expver="expver",
        leveltype="leveltype",
        ec_vars=["var1", "var2"],
        forecast_area=[0, 0, 1, 1]
    )
    assert weather_source.spark == spark

# Add more test functions as needed

if __name__ == "__main__":
    pytest.main()


class TestWeatherForecastECMWFV1Source(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local").appName("WeatherTest").getOrCreate()

    def setUp(self):
        # Initialize test data and objects
        self.save_path = "/path/to/save"
        self.date_start = "2023-08-01 00:00:00"
        self.date_end = "2023-08-02 00:00:00"
        self.ecmwf_class = "your_ecmwf_class"
        self.stream = "your_stream"
        self.expver = "your_expver"
        self.leveltype = "your_leveltype"
        self.ec_vars = ["var1", "var2"]
        self.forecast_area = [40, -10, 35, -5]  # Replace with your specific coordinates
        self.weather_source = WeatherForecastECMWFV1Source(
            self.spark,
            None,  # You can pass the appropriate SparkContext here if needed
            self.save_path,
            self.date_start,
            self.date_end,
            self.ecmwf_class,
            self.stream,
            self.expver,
            self.leveltype,
            self.ec_vars,
            self.forecast_area
        )

    def test_get_lead_time(self):
        lead_times = self.weather_source._get_lead_time()
        expected_lead_times = [0, 1, 2, ..., 243, 244, 245]
        self.assertEqual(lead_times, expected_lead_times)

    def test_get_api_params(self):
        lead_times = [0, 1, 2, 3]
        params = self.weather_source._get_api_params(lead_times)
        expected_params = {
            "class": self.ecmwf_class,
            "stream": self.stream,
            "expver": self.expver,
            "levtype": self.leveltype,
            "type": "fc",
            "param": self.ec_vars,
            "step": lead_times,
            "area": self.forecast_area,
            "grid": [0.1, 0.1]
        }
        self.assertEqual(params, expected_params)

    # You can add more test cases to cover other methods as well

if __name__ == '__main__':
    unittest.main()
