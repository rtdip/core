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

from datetime import datetime
from tracemalloc import start
from pydantic import BaseModel, Field, Extra
import strawberry
from typing import List, Union, Dict, Any
from fastapi import Query, Header, Depends
from datetime import date
from src.api.auth.azuread import oauth2_scheme

class Fields(BaseModel):
    name: str
    type: str

@strawberry.experimental.pydantic.type(model=Fields, all_fields=True)
class FieldsQL:
    pass

class FieldSchema(BaseModel):
    fields: List[Fields]
    pandas_version: str

@strawberry.type
class FieldSchemaQL:
    fields: List[FieldsQL]
    pandas_version: str

class MetadataRow(BaseModel):
    TagName: str
    UoM: str
    Description: str
    class Config:
        extra = Extra.allow

class RawRow(BaseModel):
    EventTime: datetime
    TagName: str
    Status: str
    Value: Union[float, int, str, None]

@strawberry.type
class RawRowQL:
    EventTime: datetime
    TagName: str
    Status: str
    Value: float

class MetadataResponse(BaseModel):
    field_schema: FieldSchema = Field(None, alias='schema')
    data: List[MetadataRow]

class RawResponse(BaseModel):
    field_schema: FieldSchema = Field(None, alias='schema')
    data: List[RawRow]

@strawberry.type
class RawResponseQL:
    schema: FieldSchemaQL
    data: List[RawRowQL]

class ResampleInterpolateRow(BaseModel):
    EventTime: datetime
    TagName: str
    Value: Union[float, int, str, None]

class ResampleInterpolateResponse(BaseModel):
    field_schema: FieldSchema = Field(None, alias='schema')
    data: List[ResampleInterpolateRow]

class HTTPError(BaseModel):
    detail: str

    class Config:
        schema_extra = {
            "example": {"detail": "HTTPException raised."},
        } 

class BaseQueryParams:
    def __init__(
        self,
        business_unit: str = Query(..., description="Business Unit Name"),
        region: str = Query(..., description="Region"), 
        asset: str = Query(..., description="Asset"), 
        data_security_level: str = Query(..., description="Data Security Level"),
        authorization: str = Depends(oauth2_scheme)   
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
        data_type: str = Query(..., description="Data Type"), 
        include_bad_data: bool = Query(..., description="Include or remove Bad data points"),
        start_date: Union[date, datetime] = Query(..., description="Start Date in format YYYY-MM-DD or YYYY-MM-DDTHH:mm:ss or YYYY-MM-DDTHH:mm:ss+zz:zz", examples={"2022-01-01": {"value": "2022-01-01"}, "2022-01-01T15:00:00": {"value": "2022-01-01T15:00:00"}, "2022-01-01T15:00:00+00:00": {"value": "2022-01-01T15:00:00+00:00"}}),
        end_date: Union[date, datetime] = Query(..., description="End Date in format YYYY-MM-DD or YYYY-MM-DDTHH:mm:ss or YYYY-MM-DDTHH:mm:ss+zz:zz", examples={"2022-01-02": {"value": "2022-01-02"}, "2022-01-01T16:00:00": {"value": "2022-01-01T16:00:00"}, "2022-01-01T15:00:00+00:00": {"value": "2022-01-01T15:00:00+00:00"}}),
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
        sample_rate: str = Query(..., description="Sample Rate", example=5),
        sample_unit: str = Query(..., description="Sample Unit", examples={"second": {"value": "second"}, "minute": {"value": "minute"}, "hour": {"value": "hour"}, "day": {"value": "day"}}),
        agg_method: str = Query(..., description="Aggregation Method", examples={"first": {"value": "first"}, "last": {"value": "last"}, "avg": {"value": "avg"}, "min": {"value": "min"}, "max": {"value": "max"}}),   
    ):
        self.sample_rate = sample_rate
        self.sample_unit = sample_unit
        self.agg_method = agg_method

class InterpolateQueryParams:
    def __init__(
        self,
        interpolation_method: str = Query(..., description="Interpolation Method", examples={"forward_fill": {"value": "forward_fill"}, "backward_fill": {"value": "backward_fill"}}),
    ):
        self.interpolation_method = interpolation_method

class InterpolationAtTimeQueryParams:
    def __init__(
        self,
        data_type: str = Query(..., description="Data Type"),
        timestamps: List[Union[date, datetime]] = Query(..., description="Timestamps in format YYYY-MM-DD or YYYY-MM-DDTHH:mm:ss or YYYY-MM-DDTHH:mm:ss+zz:zz", examples={"2022-01-01": {"value": "2022-01-01"}, "2022-01-01T15:00:00": {"value": "2022-01-01T15:00:00"}, "2022-01-01T15:00:00+00:00": {"value": "2022-01-01T15:00:00+00:00"}}), 
    ):
        self.data_type = data_type
        self.timestamps = timestamps

class TimeWeightedAverageQueryParams:
    def __init__(
        self,
        window_size_mins: int = Query(..., description="Window Size Mins", example=20),
        window_length: int = Query(..., description="Window Length", example=10),
        step: str = Query(..., description="Step", examples={"true": {"value": "true"}, "false": {"value": "false"}, "metadata": {"value": "metadata"}}) 
    ):
        self.window_size_mins = window_size_mins
        self.window_length = window_length
        self.step = step