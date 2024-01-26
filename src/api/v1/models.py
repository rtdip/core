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
from datetime import datetime
from tracemalloc import start
from pydantic import BaseModel, ConfigDict, Field, field_serializer
from typing import List, Union, Dict, Any
from fastapi import Query, Header, Depends
from datetime import date
from src.api.auth.azuread import oauth2_scheme

EXAMPLE_DATE = "2022-01-01"
EXAMPLE_DATETIME = "2022-01-01T15:00:00"
EXAMPLE_DATETIME_TIMEZOME = "2022-01-01T15:00:00+00:00"


class DuplicatedQueryParameters:
    time_interval_rate = Query(
        ..., description="Time Interval Rate as a numeric input", examples=[5]
    )
    time_interval_unit = Query(
        ...,
        description="Time Interval Unit can be one of the options: [second, minute, day, hour]",
        examples=["second", "minute", "hour", "day"],
    )


class Fields(BaseModel):
    name: str
    type: str


class FieldSchema(BaseModel):
    fields: List[Fields]
    pandas_version: str


class MetadataRow(BaseModel):
    TagName: str
    UoM: str
    Description: str

    class Config:
        extra = "allow"


class LatestRow(BaseModel):
    TagName: str
    EventTime: datetime
    Status: str
    Value: Union[str, None]
    ValueType: Union[str, None]
    GoodEventTime: Union[datetime, None]
    GoodValue: Union[str, None]
    GoodValueType: Union[str, None]


class RawRow(BaseModel):
    EventTime: datetime
    TagName: str
    Status: str
    Value: Union[float, int, str, None]


class SummaryRow(BaseModel):
    TagName: str
    Count: Union[float, int, None]
    Avg: Union[float, int, None]
    Min: Union[float, int, None]
    Max: Union[float, int, None]
    StDev: Union[float, int, None]
    Sum: Union[float, int, None]
    Var: Union[float, int, None]


class MetadataResponse(BaseModel):
    field_schema: FieldSchema = Field(
        None, alias="schema", serialization_alias="schema"
    )
    data: List[MetadataRow]


class LatestResponse(BaseModel):
    field_schema: FieldSchema = Field(
        None, alias="schema", serialization_alias="schema"
    )
    data: List[LatestRow]


class RawResponse(BaseModel):
    field_schema: FieldSchema = Field(
        None, alias="schema", serialization_alias="schema"
    )
    data: List[RawRow]


class ResampleInterpolateRow(BaseModel):
    EventTime: datetime
    TagName: str
    Value: Union[float, int, str, None]


class PivotRow(BaseModel):
    EventTime: datetime

    model_config = ConfigDict(extra="allow")


class ResampleInterpolateResponse(BaseModel):
    field_schema: FieldSchema = Field(
        None, alias="schema", serialization_alias="schema"
    )
    data: List[ResampleInterpolateRow]


class SummaryResponse(BaseModel):
    field_schema: FieldSchema = Field(
        None, alias="schema", serialization_alias="schema"
    )
    data: List[SummaryRow]


class PivotResponse(BaseModel):
    field_schema: FieldSchema = Field(
        None, alias="schema", serialization_alias="schema"
    )
    data: List[PivotRow]


class HTTPError(BaseModel):
    detail: str

    class Config:
        schema_extra = (
            {
                "example": {"detail": "HTTPException raised."},
            },
        )


class BaseHeaders:
    def __init__(
        self,
        x_databricks_server_hostname: str = Header(
            default=(
                ...
                if os.getenv("DATABRICKS_SQL_SERVER_HOSTNAME") is None
                else os.getenv("DATABRICKS_SQL_SERVER_HOSTNAME")
            ),
            description="Databricks SQL Server Hostname",
            include_in_schema=(
                True if os.getenv("DATABRICKS_SQL_SERVER_HOSTNAME") is None else False
            ),
        ),
        x_databricks_http_path: str = Header(
            default=(
                ...
                if os.getenv("DATABRICKS_SQL_HTTP_PATH") is None
                else os.getenv("DATABRICKS_SQL_HTTP_PATH")
            ),
            description="Databricks SQL HTTP Path",
            include_in_schema=(
                True if os.getenv("DATABRICKS_SQL_HTTP_PATH") is None else False
            ),
        ),
    ):
        self.x_databricks_server_hostname = x_databricks_server_hostname
        self.x_databricks_http_path = x_databricks_http_path


class BaseQueryParams:
    def __init__(
        self,
        business_unit: str = Query(..., description="Business Unit Name"),
        region: str = Query(..., description="Region"),
        asset: str = Query(..., description="Asset"),
        data_security_level: str = Query(..., description="Data Security Level"),
        authorization: str = Depends(oauth2_scheme),
    ):
        self.business_unit = business_unit
        self.region = region
        self.asset = asset
        self.data_security_level = data_security_level
        self.authorization = authorization


class MetadataQueryParams:
    def __init__(
        self,
        tag_name: List[str] = Query(None, description="Tag Name"),
    ):
        self.tag_name = tag_name


class RawQueryParams:
    def __init__(
        self,
        data_type: str = Query(
            ...,
            description="Data Type can be one of the following options: float, double, integer, string",
            examples=["float", "double", "integer", "string"],
        ),
        include_bad_data: bool = Query(
            ..., description="Include or remove Bad data points"
        ),
        start_date: Union[date, datetime] = Query(
            ...,
            description="Start Date in format YYYY-MM-DD or YYYY-MM-DDTHH:mm:ss or YYYY-MM-DDTHH:mm:ss+zz:zz",
            examples=[EXAMPLE_DATE, EXAMPLE_DATETIME, EXAMPLE_DATETIME_TIMEZOME],
        ),
        end_date: Union[date, datetime] = Query(
            ...,
            description="End Date in format YYYY-MM-DD or YYYY-MM-DDTHH:mm:ss or YYYY-MM-DDTHH:mm:ss+zz:zz",
            examples=[EXAMPLE_DATE, EXAMPLE_DATETIME, EXAMPLE_DATETIME_TIMEZOME],
        ),
    ):
        self.data_type = data_type
        self.include_bad_data = include_bad_data
        self.start_date = start_date
        self.end_date = end_date


class TagsQueryParams:
    def __init__(
        self,
        tag_name: List[str] = Query(..., description="Tag Name"),
    ):
        self.tag_name = tag_name


class TagsBodyParams(BaseModel):
    tag_name: List[str]


class ResampleQueryParams:
    def __init__(
        self,
        sample_rate: str = Query(
            ...,
            description="sample_rate is deprecated and will be removed in v1.0.0. Please use time_interval_rate instead.",
            examples=[5],
            deprecated=True,
        ),
        sample_unit: str = Query(
            ...,
            description="sample_unit is deprecated and will be removed in v1.0.0. Please use time_interval_unit instead.",
            examples=["second", "minute", "hour", "day"],
            deprecated=True,
        ),
        time_interval_rate: str = DuplicatedQueryParameters.time_interval_rate,
        time_interval_unit: str = DuplicatedQueryParameters.time_interval_unit,
        agg_method: str = Query(
            ...,
            description="Aggregation Method can be one of the following [first, last, avg, min, max]",
            examples=["first", "last", "avg", "min", "max"],
        ),
    ):
        self.sample_rate = sample_rate
        self.sample_unit = sample_unit
        self.time_interval_rate = time_interval_rate
        self.time_interval_unit = time_interval_unit
        self.agg_method = agg_method


class PivotQueryParams:
    def __init__(
        self,
        pivot: bool = Query(
            default=False,
            description="Pivot the data on timestamp column with True or do not pivot the data with False",
        ),
    ):
        self.pivot = pivot


class LimitOffsetQueryParams:
    def __init__(
        self,
        limit: int = Query(
            default=None,
            description="The number of rows to be returned",
        ),
        offset: int = Query(
            default=None,
            description="The number of rows to skip before returning rows",
        ),
    ):
        self.limit = limit
        self.offset = offset


class InterpolateQueryParams:
    def __init__(
        self,
        interpolation_method: str = Query(
            ...,
            description="Interpolation Method can e one of the following [forward_fill, backward_fill, linear]",
            examples=["forward_fill", "backward_fill", "linear"],
        ),
    ):
        self.interpolation_method = interpolation_method


class InterpolationAtTimeQueryParams:
    def __init__(
        self,
        data_type: str = Query(
            ...,
            description="Data Type can be one of the following options:[float, double, integer, string]",
        ),
        timestamps: List[Union[date, datetime]] = Query(
            ...,
            description="Timestamps in format YYYY-MM-DD or YYYY-MM-DDTHH:mm:ss or YYYY-MM-DDTHH:mm:ss+zz:zz",
            examples=[EXAMPLE_DATE, EXAMPLE_DATETIME, EXAMPLE_DATETIME_TIMEZOME],
        ),
        window_length: int = Query(
            ..., description="Window Length in days", examples=[1]
        ),
        include_bad_data: bool = Query(
            ..., description="Include or remove Bad data points"
        ),
    ):
        self.data_type = data_type
        self.timestamps = timestamps
        self.window_length = window_length
        self.include_bad_data = include_bad_data


class TimeWeightedAverageQueryParams:
    def __init__(
        self,
        window_size_mins: int = Query(
            ...,
            description="window_size_mins is deprecated and will be removed in v1.0.0. Please use time_interval_rate and time_interval_unit instead.",
            examples=[20],
            deprecated=True,
        ),
        time_interval_rate: str = DuplicatedQueryParameters.time_interval_rate,
        time_interval_unit: str = DuplicatedQueryParameters.time_interval_unit,
        window_length: int = Query(
            ..., description="Window Length in days", examples=[1]
        ),
        step: str = Query(
            ...,
            description='Step can be "true", "false" or "metadata". "metadata" will retrieve the step value from the metadata table.',
            examples=["true", "false", "metadata"],
        ),
    ):
        self.window_size_mins = window_size_mins
        self.time_interval_rate = time_interval_rate
        self.time_interval_unit = time_interval_unit
        self.window_length = window_length
        self.step = step


class CircularAverageQueryParams:
    def __init__(
        self,
        time_interval_rate: str = DuplicatedQueryParameters.time_interval_rate,
        time_interval_unit: str = DuplicatedQueryParameters.time_interval_unit,
        lower_bound: int = Query(
            ..., description="Lower boundary for the sample range", examples=[5]
        ),
        upper_bound: int = Query(
            ..., description="Upper boundary for the sample range", examples=[20]
        ),
    ):
        self.time_interval_rate = time_interval_rate
        self.time_interval_unit = time_interval_unit
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound
