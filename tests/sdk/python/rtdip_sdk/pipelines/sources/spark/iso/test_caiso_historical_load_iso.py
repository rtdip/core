# Copyright 2022 RTDIP
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
import os
import sys
from io import StringIO

import pandas as pd
from requests import HTTPError

sys.path.insert(0, ".")
import pytest
from src.sdk.python.rtdip_sdk.pipelines.sources.spark.iso import CAISOHistoricalLoadISOSource
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.iso import CAISO_SCHEMA
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries
from pyspark.sql import DataFrame, SparkSession
from pytest_mock import MockerFixture

dir_path = os.path.dirname(os.path.realpath(__file__))
area_filter = "TacAreaName = 'AVA'"

iso_configuration = {
    "load_types": ["Total Actual Hourly Integrated Load"],
    "start_date": "2023-11-08",
    "end_date": "2023-11-09",
}

expected_forecast_data = (
    """StartTime,EndTime,LoadType,OprDt,OprHr,OprInterval,MarketRunId,TacAreaName,Label,XmlDataItem,Pos,Load,ExecutionType,Group
    2023-11-08T05:00:00,2023-11-08T06:00:00,2,2023-11-07,22,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1204.52,2DA,1
    2023-11-08T00:00:00,2023-11-08T01:00:00,2,2023-11-07,17,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1362.97,2DA,1
    2023-11-08T07:00:00,2023-11-08T08:00:00,2,2023-11-07,24,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1043.01,2DA,1
    2023-11-08T02:00:00,2023-11-08T03:00:00,2,2023-11-07,19,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1383.38,2DA,1
    2023-11-08T03:00:00,2023-11-08T04:00:00,2,2023-11-07,20,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1344.49,2DA,1
    2023-11-08T06:00:00,2023-11-08T07:00:00,2,2023-11-07,23,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1112.24,2DA,1
    2023-11-08T04:00:00,2023-11-08T05:00:00,2,2023-11-07,21,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1289.55,2DA,1
    2023-11-08T01:00:00,2023-11-08T02:00:00,2,2023-11-07,18,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1410.66,2DA,1
    2023-11-08T13:00:00,2023-11-08T14:00:00,2,2023-11-08,6,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1171.48,2DA,208
    2023-11-08T23:00:00,2023-11-09T00:00:00,2,2023-11-08,16,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1287.42,2DA,208
    2023-11-09T06:00:00,2023-11-09T07:00:00,2,2023-11-08,23,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1169.33,2DA,208
    2023-11-08T10:00:00,2023-11-08T11:00:00,2,2023-11-08,3,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,984.5,2DA,208
    2023-11-08T17:00:00,2023-11-08T18:00:00,2,2023-11-08,10,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1357.92,2DA,208
    2023-11-08T18:00:00,2023-11-08T19:00:00,2,2023-11-08,11,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1332.09,2DA,208
    2023-11-09T00:00:00,2023-11-09T01:00:00,2,2023-11-08,17,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1368.84,2DA,208
    2023-11-08T22:00:00,2023-11-08T23:00:00,2,2023-11-08,15,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1271.01,2DA,208
    2023-11-08T15:00:00,2023-11-08T16:00:00,2,2023-11-08,8,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1390.17,2DA,208
    2023-11-09T04:00:00,2023-11-09T05:00:00,2,2023-11-08,21,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1337.45,2DA,208
    2023-11-08T19:00:00,2023-11-08T20:00:00,2,2023-11-08,12,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1308.97,2DA,208
    2023-11-08T12:00:00,2023-11-08T13:00:00,2,2023-11-08,5,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1048.27,2DA,208
    2023-11-08T16:00:00,2023-11-08T17:00:00,2,2023-11-08,9,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1381.9,2DA,208
    2023-11-08T14:00:00,2023-11-08T15:00:00,2,2023-11-08,7,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1319.1,2DA,208
    2023-11-08T21:00:00,2023-11-08T22:00:00,2,2023-11-08,14,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1278.99,2DA,208
    2023-11-09T03:00:00,2023-11-09T04:00:00,2,2023-11-08,20,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1385.29,2DA,208
    2023-11-09T01:00:00,2023-11-09T02:00:00,2,2023-11-08,18,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1429.2,2DA,208
    2023-11-08T11:00:00,2023-11-08T12:00:00,2,2023-11-08,4,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,995.53,2DA,208
    2023-11-09T05:00:00,2023-11-09T06:00:00,2,2023-11-08,22,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1259.3,2DA,208
    2023-11-08T09:00:00,2023-11-08T10:00:00,2,2023-11-08,2,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,990.75,2DA,208
    2023-11-08T20:00:00,2023-11-08T21:00:00,2,2023-11-08,13,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1294.13,2DA,208
    2023-11-08T08:00:00,2023-11-08T09:00:00,2,2023-11-08,1,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1010.8,2DA,208
    2023-11-09T02:00:00,2023-11-09T03:00:00,2,2023-11-08,19,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1414.12,2DA,208
    2023-11-09T07:00:00,2023-11-09T08:00:00,2,2023-11-08,24,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1102.91,2DA,208
    2023-11-09T12:00:00,2023-11-09T13:00:00,2,2023-11-09,5,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1129.88,2DA,415
    2023-11-09T15:00:00,2023-11-09T16:00:00,2,2023-11-09,8,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1474.35,2DA,415
    2023-11-09T13:00:00,2023-11-09T14:00:00,2,2023-11-09,6,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1254.17,2DA,415
    2023-11-09T11:00:00,2023-11-09T12:00:00,2,2023-11-09,4,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1076.1,2DA,415
    2023-11-09T16:00:00,2023-11-09T17:00:00,2,2023-11-09,9,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1458.9,2DA,415
    2023-11-09T09:00:00,2023-11-09T10:00:00,2,2023-11-09,2,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1067.57,2DA,415
    2023-11-09T19:00:00,2023-11-09T20:00:00,2,2023-11-09,12,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1365.78,2DA,415
    2023-11-09T17:00:00,2023-11-09T18:00:00,2,2023-11-09,10,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1428.04,2DA,415
    2023-11-09T14:00:00,2023-11-09T15:00:00,2,2023-11-09,7,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1404.61,2DA,415
    2023-11-09T23:00:00,2023-11-10T00:00:00,2,2023-11-09,16,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1316.18,2DA,415
    2023-11-09T21:00:00,2023-11-09T22:00:00,2,2023-11-09,14,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1324.18,2DA,415
    2023-11-09T10:00:00,2023-11-09T11:00:00,2,2023-11-09,3,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1063.02,2DA,415
    2023-11-09T20:00:00,2023-11-09T21:00:00,2,2023-11-09,13,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1344.52,2DA,415
    2023-11-09T08:00:00,2023-11-09T09:00:00,2,2023-11-09,1,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1085.77,2DA,415
    2023-11-09T22:00:00,2023-11-09T23:00:00,2,2023-11-09,15,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1311.1,2DA,415
    2023-11-09T18:00:00,2023-11-09T19:00:00,2,2023-11-09,11,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1395.33,2DA,415
    """
)

expected_actual_data = (
    """StartTime,EndTime,LoadType,OprDt,OprHr,OprInterval,MarketRunId,TacAreaName,Label,XmlDataItem,Pos,Load,ExecutionType,Group
    2023-11-08T04:00:00,2023-11-08T05:00:00,0,2023-11-07,21,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1300.0,ACTUAL,69
    2023-11-08T01:00:00,2023-11-08T02:00:00,0,2023-11-07,18,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1399.0,ACTUAL,69
    2023-11-08T06:00:00,2023-11-08T07:00:00,0,2023-11-07,23,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1142.0,ACTUAL,69
    2023-11-08T05:00:00,2023-11-08T06:00:00,0,2023-11-07,22,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1226.0,ACTUAL,69
    2023-11-08T02:00:00,2023-11-08T03:00:00,0,2023-11-07,19,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1368.0,ACTUAL,69
    2023-11-08T07:00:00,2023-11-08T08:00:00,0,2023-11-07,24,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1083.0,ACTUAL,69
    2023-11-08T00:00:00,2023-11-08T01:00:00,0,2023-11-07,17,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1347.0,ACTUAL,69
    2023-11-08T03:00:00,2023-11-08T04:00:00,0,2023-11-07,20,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1343.0,ACTUAL,69
    2023-11-08T12:00:00,2023-11-08T13:00:00,0,2023-11-08,5,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1108.0,ACTUAL,276
    2023-11-09T01:00:00,2023-11-09T02:00:00,0,2023-11-08,18,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1428.0,ACTUAL,276
    2023-11-08T19:00:00,2023-11-08T20:00:00,0,2023-11-08,12,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1333.0,ACTUAL,276
    2023-11-09T06:00:00,2023-11-09T07:00:00,0,2023-11-08,23,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1220.0,ACTUAL,276
    2023-11-08T20:00:00,2023-11-08T21:00:00,0,2023-11-08,13,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1297.0,ACTUAL,276
    2023-11-08T21:00:00,2023-11-08T22:00:00,0,2023-11-08,14,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1276.0,ACTUAL,276
    2023-11-09T03:00:00,2023-11-09T04:00:00,0,2023-11-08,20,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1409.0,ACTUAL,276
    2023-11-08T09:00:00,2023-11-08T10:00:00,0,2023-11-08,2,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1042.0,ACTUAL,276
    2023-11-08T13:00:00,2023-11-08T14:00:00,0,2023-11-08,6,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1236.0,ACTUAL,276
    2023-11-08T15:00:00,2023-11-08T16:00:00,0,2023-11-08,8,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1446.0,ACTUAL,276
    2023-11-09T00:00:00,2023-11-09T01:00:00,0,2023-11-08,17,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1356.0,ACTUAL,276
    2023-11-09T02:00:00,2023-11-09T03:00:00,0,2023-11-08,19,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1427.0,ACTUAL,276
    2023-11-08T10:00:00,2023-11-08T11:00:00,0,2023-11-08,3,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1037.0,ACTUAL,276
    2023-11-08T17:00:00,2023-11-08T18:00:00,0,2023-11-08,10,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1415.0,ACTUAL,276
    2023-11-08T11:00:00,2023-11-08T12:00:00,0,2023-11-08,4,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1053.0,ACTUAL,276
    2023-11-08T14:00:00,2023-11-08T15:00:00,0,2023-11-08,7,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1385.0,ACTUAL,276
    2023-11-08T22:00:00,2023-11-08T23:00:00,0,2023-11-08,15,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1259.0,ACTUAL,276
    2023-11-08T18:00:00,2023-11-08T19:00:00,0,2023-11-08,11,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1376.0,ACTUAL,276
    2023-11-08T23:00:00,2023-11-09T00:00:00,0,2023-11-08,16,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1267.0,ACTUAL,276
    2023-11-09T04:00:00,2023-11-09T05:00:00,0,2023-11-08,21,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1365.0,ACTUAL,276
    2023-11-09T05:00:00,2023-11-09T06:00:00,0,2023-11-08,22,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1299.0,ACTUAL,276
    2023-11-09T07:00:00,2023-11-09T08:00:00,0,2023-11-08,24,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1155.0,ACTUAL,276
    2023-11-08T08:00:00,2023-11-08T09:00:00,0,2023-11-08,1,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1052.0,ACTUAL,276
    2023-11-08T16:00:00,2023-11-08T17:00:00,0,2023-11-08,9,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1437.0,ACTUAL,276
    2023-11-09T18:00:00,2023-11-09T19:00:00,0,2023-11-09,11,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1447.0,ACTUAL,483
    2023-11-09T17:00:00,2023-11-09T18:00:00,0,2023-11-09,10,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1477.0,ACTUAL,483
    2023-11-09T20:00:00,2023-11-09T21:00:00,0,2023-11-09,13,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1383.0,ACTUAL,483
    2023-11-09T14:00:00,2023-11-09T15:00:00,0,2023-11-09,7,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1438.0,ACTUAL,483
    2023-11-09T08:00:00,2023-11-09T09:00:00,0,2023-11-09,1,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1113.0,ACTUAL,483
    2023-11-09T11:00:00,2023-11-09T12:00:00,0,2023-11-09,4,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1108.0,ACTUAL,483
    2023-11-09T12:00:00,2023-11-09T13:00:00,0,2023-11-09,5,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1162.0,ACTUAL,483
    2023-11-09T16:00:00,2023-11-09T17:00:00,0,2023-11-09,9,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1509.0,ACTUAL,483
    2023-11-09T09:00:00,2023-11-09T10:00:00,0,2023-11-09,2,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1100.0,ACTUAL,483
    2023-11-09T13:00:00,2023-11-09T14:00:00,0,2023-11-09,6,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1286.0,ACTUAL,483
    2023-11-09T15:00:00,2023-11-09T16:00:00,0,2023-11-09,8,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1513.0,ACTUAL,483
    2023-11-09T10:00:00,2023-11-09T11:00:00,0,2023-11-09,3,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1095.0,ACTUAL,483
    2023-11-09T21:00:00,2023-11-09T22:00:00,0,2023-11-09,14,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1366.0,ACTUAL,483
    2023-11-09T23:00:00,2023-11-10T00:00:00,0,2023-11-09,16,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1360.0,ACTUAL,483
    2023-11-09T19:00:00,2023-11-09T20:00:00,0,2023-11-09,12,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1413.0,ACTUAL,483
    2023-11-09T22:00:00,2023-11-09T23:00:00,0,2023-11-09,15,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1358.0,ACTUAL,483
    """
)

patch_module_name = "requests.get"


def test_caiso_historical_load_iso_read_setup(spark_session: SparkSession):
    iso_source = CAISOHistoricalLoadISOSource(spark_session, iso_configuration)

    assert iso_source.system_type().value == 2
    assert iso_source.libraries() == Libraries(
        maven_libraries=[], pypi_libraries=[], pythonwheel_libraries=[]
    )

    assert isinstance(iso_source.settings(), dict)

    assert sorted(iso_source.required_options) == ["end_date", "load_types", "start_date"]
    assert iso_source.pre_read_validation()


def test_caiso_historical_load_iso_read_batch_actual(spark_session: SparkSession, mocker: MockerFixture):
    iso_source = CAISOHistoricalLoadISOSource(spark_session, {**iso_configuration,
                                                              "load_types": ["Total Actual Hourly Integrated Load"]})

    with open(f"{dir_path}/data/caiso_historical_load_sample1.zip", "rb") as file:
        sample_bytes = file.read()

    class MyResponse:
        content = sample_bytes
        status_code = 200

    def get_response(url: str):
        assert url.startswith("http://oasis.caiso.com/oasisapi/SingleZip")
        return MyResponse()

    mocker.patch(patch_module_name, side_effect=get_response)

    df = iso_source.read_batch()
    df = df.filter(area_filter)

    assert df.count() == 48
    assert isinstance(df, DataFrame)
    assert str(df.schema) == str(CAISO_SCHEMA)

    expected_df_spark = spark_session.createDataFrame(
        pd.read_csv(StringIO(expected_actual_data), parse_dates=["StartTime", "EndTime"]),
        schema=CAISO_SCHEMA)

    cols = df.columns
    assert df.orderBy(cols).collect() == expected_df_spark.orderBy(cols).collect()


def test_caiso_historical_load_iso_read_batch_forecast(spark_session: SparkSession, mocker: MockerFixture):
    iso_source = CAISOHistoricalLoadISOSource(spark_session, {**iso_configuration,
                                                              "load_types": ["Demand Forecast 2-Day Ahead"]})

    with open(f"{dir_path}/data/caiso_historical_load_sample1.zip", "rb") as file:
        sample_bytes = file.read()

    class MyResponse:
        content = sample_bytes
        status_code = 200

    def get_response(url: str):
        assert url.startswith("http://oasis.caiso.com/oasisapi/SingleZip")
        return MyResponse()

    mocker.patch(patch_module_name, side_effect=get_response)

    df = iso_source.read_batch()
    df = df.filter(area_filter)

    assert df.count() == 48
    assert isinstance(df, DataFrame)
    assert str(df.schema) == str(CAISO_SCHEMA)

    expected_df_spark = spark_session.createDataFrame(
        pd.read_csv(StringIO(expected_forecast_data), parse_dates=["StartTime", "EndTime"]),
        schema=CAISO_SCHEMA)

    cols = df.columns
    assert df.orderBy(cols).collect() == expected_df_spark.orderBy(cols).collect()


def test_caiso_historical_load_iso_iso_invalid_dates(spark_session: SparkSession):
    with pytest.raises(ValueError) as exc_info:
        iso_source = CAISOHistoricalLoadISOSource(
            spark_session, {**iso_configuration, "start_date": "2023/11/01"}
        )
        iso_source.pre_read_validation()

    expected = "Unable to parse start_date. Please specify in %Y-%m-%d format."
    assert str(exc_info.value) == expected

    with pytest.raises(ValueError) as exc_info:
        iso_source = CAISOHistoricalLoadISOSource(
            spark_session, {**iso_configuration, "end_date": "2023/11/01"}
        )
        iso_source.pre_read_validation()

    expected = "Unable to parse end_date. Please specify in %Y-%m-%d format."
    assert str(exc_info.value) == expected
