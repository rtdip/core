from datetime import datetime
import logging
import time
import pandas as pd
import numpy as np
import requests
from pyspark.sql import SparkSession
from datetime import timedelta
from io import BytesIO
from .pjm_daily_pricing_iso import PJMDailyPricingISOSource
from ...._pipeline_utils.iso import PJM_PRICING_SCHEMA
from . import BaseISOSource

class PJMHistoricalPricingISOSource(PJMDailyPricingISOSource):
    """
    The PJM Historical Pricing ISO Source is used to read historical Real-Time and Day-Ahead hourly data from PJM API. It supports both Real-Time Hourly LMPs and Day-Ahead Hourly LMPs data. 

    API:              <a href="https://api.pjm.com/api/v1/">  (must be a valid apy key from PJM)

    Real-Time doc:    <a href="https://dataminer2.pjm.com/feed/da_hrl_lmps/definition">

    Day-Ahead doc:    <a href="https://dataminer2.pjm.com/feed/rt_hrl_lmps/definition">
    
    The PJM Historical Pricing ISO Source is used same PJM endpoint, but it is called within a range established by the start_date & end_date attributes for historical data.
    
    Parameters:
        spark (SparkSession): Spark Session instance
        options (dict): A dictionary of ISO Source specific configurations (See Attributes table below)

    Attributes:
        api_key (str): Must be a valid key from PJM, see api url
        load_type (str): Must be one of `real_time` or `day_ahead`
        start_date (str): Must be valid start_date for historical data
        start_date (str): Must be valid end_date for historical data

    Please check the BaseISOSource for available methods.

    BaseISOSource:
        ::: src.sdk.python.rtdip_sdk.pipelines.sources.spark.iso.pjm_daily_pricing_iso.py
    """

    spark: SparkSession
    options: dict
    required_options = ["api_key", "load_type", "start_date", "end_date"]

    def __init__(self, spark: SparkSession, options: dict) -> None:
        super().__init__(spark, options)
        self.spark: SparkSession = spark
        self.options: dict = options
        self.start_date: str = self.options.get("start_date", "")
        self.end_date: str = self.options.get("end_date", "")
        self.user_datetime_format = "%Y-%m-%d"


    def _pull_data(self) -> pd.DataFrame:
        """
        Pulls data from the PJM API and parses the return.

        Returns:
            Raw form of data.
        """
       
        logging.info(f"Historical data requested from {self.start_date} to {self.end_date}")
        
        start_date_str = datetime.strptime(self.start_date, self.user_datetime_format).replace(hour=0, minute=0)
        end_date_str = datetime.strptime(self.end_date, self.user_datetime_format).replace(hour=23)
                    
        if self.load_type == 'day_ahead':
            url_suffix = 'da_hrl_lmps'
        else:
            url_suffix = 'rt_hrl_lmps'
        

        data = self._fetch_paginated_data(url_suffix, start_date_str, end_date_str)
            
        df = pd.DataFrame(data)
        logging.info(f"Data fetched successfully: {len(df)} rows")
        
            
        return df 
            
    def _validate_options(self) -> bool:
        super()._validate_options()
        """
        Validates all parameters including the following examples:
            - `start_date` & `end_data` must be in the correct format.
            - `start_date` must be behind `end_data`.
            - `start_date` must not be in the future (UTC).

        Returns:
            True if all looks good otherwise raises Exception.

        """

        try:
            start_date = datetime.strptime(self.start_date, self.user_datetime_format)
        except ValueError:
            raise ValueError(
                f"Unable to parse Start date. Please specify in {self.user_datetime_format} format."
            )

        try:
            end_date = datetime.strptime(self.end_date, self.user_datetime_format)
        except ValueError:
            raise ValueError(
                f"Unable to parse End date. Please specify in {self.user_datetime_format} format."
            )

        if start_date > datetime.utcnow() - timedelta(days=1):
            raise ValueError("Start date can't be in future.")

        if start_date > end_date:
            raise ValueError("Start date can't be ahead of End date.")

        if end_date > datetime.utcnow() - timedelta(days=1):
            raise ValueError("End date can't be in future.")

        return True
