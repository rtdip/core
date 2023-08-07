# -*- coding: utf-8 -*-
""" Test - Download data from the ecmwf mars server using their python api."""

__author__ = ["Amber-Rigg"]

import os

from src.sdk.python.rtdip_sdk.pipelines.sources.spark.weather.ECMWF.MARS_api_download import MARSDownloader
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries

from pyspark.sql import DataFrame, SparkSession
from pytest_mock import MockerFixture

load_dotenv()

class TestMarsDownloader:
    """Test the MARSDownloader class."""

    


