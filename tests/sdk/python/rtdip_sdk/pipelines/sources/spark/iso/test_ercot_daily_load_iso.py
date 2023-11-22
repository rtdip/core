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

sys.path.insert(0, ".")
import pandas as pd
from requests import HTTPError

import pytest
from pyspark.sql import DataFrame, SparkSession
from pytest_mock import MockerFixture

from src.sdk.python.rtdip_sdk.pipelines.sources.spark.iso import ERCOTDailyLoadISOSource
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.iso import ERCOT_SCHEMA
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries

dir_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data")

iso_configuration = {
    "load_type": "actual",
    "date": "2023-11-17",
    "certificate_pfx_key": "SOME_KEY",
    "certificate_pfx_key_contents": "SOME_DATA",
}

patch_module_name = "requests.get"

url_actual = "https://mis.ercot.com/misapp/GetReports.do?reportTypeId=13101"


# Had to put HTML Content in the Python code as Sonar was raising Bugs/Code-Smells while putting it in an HTML file.

urls_content_actual = """<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 
Transitional//EN""http://www.w3.org/TR/html4/loose.dtd"><html><head><title>ERCOT MIS</title><meta 
http-equiv="Content-Type" content="text/html"; charset="utf-8"><meta http-equiv="pragma" content="no-cache"><meta 
http-equiv="cache-control" content="no-cache"><meta http-equiv="expires" content="0"><base 
href="https://mis.ercot.com:443/misapp/"><link href="/misapp/css/vdg.css" rel="stylesheet" type="text/css"><script 
language="javascript" src="/misapp/scripts/portletTypeA.js"></script></head><body><form name="PortletTypeAForm" 
method="GET" action="/misapp/GetReports.do;jsessionid=43DA810ED9494704D1109C6976249C8B.w_prblwbc0012d_e2"><input 
type="hidden" name="basePath" id ="basePath" value="https://mis.ercot.com:443/misapp/"/><table width='800' border='0' 
cellpadding='0' cellspacing='0' class='tbl_ltTaupe'><tr><td class='hdr_grey'><table width='100%' border='0' 
cellspacing='0' cellpadding='0><tr class='hdr_grey'><td><b>Actual System Load by Weather Zone</b></td><td><div 
align='right'></div></td></tr></table></td></tr><tr><td><table width='800' border='0' cellspacing='0' 
cellpadding='4'><tr><td class='labelRequired_ind'><b>Title</b></td><td  class='labelRequired'><div 
align='center'><b>XML</b></div></td><td  class='labelRequired'><div align='center'><b>CSV</b></div></td><td  
class='labelRequired'><div align='center'><b>Other</b></div></td></tr><tr><td 
class='labelOptional_ind'>cdr.00013101.0000000000000000.20231120.055000675.ACTUALSYSLOADWZNP6345_csv.zip</td><td  
class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div align='center'></div></td><td  
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959561638'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231120.055000624.ACTUALSYSLOADWZNP6345_xml.zip</td><td 
 class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div align='center'></div></td><td  
 class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959561637'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231119.055001313.ACTUALSYSLOADWZNP6345_csv.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959328743'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231119.055001249.ACTUALSYSLOADWZNP6345_xml.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959328742'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231118.055000923.ACTUALSYSLOADWZNP6345_csv.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959096531'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231118.055000878.ACTUALSYSLOADWZNP6345_xml.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959096849'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231117.055001328.ACTUALSYSLOADWZNP6345_csv.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=958853867'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231117.055001289.ACTUALSYSLOADWZNP6345_xml.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=958853866'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231116.055001166.ACTUALSYSLOADWZNP6345_csv.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=958614246'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231116.055001116.ACTUALSYSLOADWZNP6345_xml.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=958614245'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231115.055000366.ACTUALSYSLOADWZNP6345_csv.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=958373072'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231115.055000320.ACTUALSYSLOADWZNP6345_xml.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=958373071'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231114.055001063.ACTUALSYSLOADWZNP6345_csv.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=958129766'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231114.055001023.ACTUALSYSLOADWZNP6345_xml.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=958129750'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231113.055000961.ACTUALSYSLOADWZNP6345_csv.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=957880669'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231113.055000899.ACTUALSYSLOADWZNP6345_xml.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=957880901'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231112.055000824.ACTUALSYSLOADWZNP6345_csv.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=957649761'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231112.055000755.ACTUALSYSLOADWZNP6345_xml.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=957649760'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231111.055001050.ACTUALSYSLOADWZNP6345_csv.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=957418948'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231111.055000997.ACTUALSYSLOADWZNP6345_xml.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=957418971'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231110.055000881.ACTUALSYSLOADWZNP6345_csv.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=957179691'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231110.055000807.ACTUALSYSLOADWZNP6345_xml.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=957179497'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231109.055001496.ACTUALSYSLOADWZNP6345_csv.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=956940466'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231109.055001428.ACTUALSYSLOADWZNP6345_xml.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=956940465'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231108.055000833.ACTUALSYSLOADWZNP6345_csv.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=956701546'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231108.055000763.ACTUALSYSLOADWZNP6345_xml.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=956701545'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231107.055001060.ACTUALSYSLOADWZNP6345_csv.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=956461305'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231107.055000955.ACTUALSYSLOADWZNP6345_xml.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=956461640'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231106.055001015.ACTUALSYSLOADWZNP6345_csv.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=956215665'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231106.055000774.ACTUALSYSLOADWZNP6345_xml.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=956215796'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231105.055000406.ACTUALSYSLOADWZNP6345_csv.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=955987594'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231105.055000352.ACTUALSYSLOADWZNP6345_xml.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=955987593'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231104.055000981.ACTUALSYSLOADWZNP6345_csv.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=955750502'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231104.055000902.ACTUALSYSLOADWZNP6345_xml.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=955750501'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231103.055000827.ACTUALSYSLOADWZNP6345_csv.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=955513781'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231103.055000785.ACTUALSYSLOADWZNP6345_xml.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=955513780'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231102.055000881.ACTUALSYSLOADWZNP6345_csv.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=955275537'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231102.055000837.ACTUALSYSLOADWZNP6345_xml.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=955275536'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231101.055000731.ACTUALSYSLOADWZNP6345_csv.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=955035870'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231101.055000678.ACTUALSYSLOADWZNP6345_xml.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=955036097'>zip</a></div></td></tr
 ></table></div><table width='100%'><tr><td class='blue_expand_bar'><b><img id='img102023' 
 src='images/ico_arrow_collapse.gif' width='20' height='16' onclick="showHideDiv('div102023','img102023')">October 
 2023</b></td></tr></table><div style='display:none' id='div102023'><table width='100%'><tr><td 
 class='labelOptional_ind'>cdr.00013101.0000000000000000.20231031.055000932.ACTUALSYSLOADWZNP6345_csv.zip</td><td  
 class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div align='center'></div></td><td  
 class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=954794955'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231031.055000871.ACTUALSYSLOADWZNP6345_xml.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=954794953'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231030.055000810.ACTUALSYSLOADWZNP6345_csv.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=954548667'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231030.055000745.ACTUALSYSLOADWZNP6345_xml.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=954548665'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231029.055000672.ACTUALSYSLOADWZNP6345_csv.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=954319216'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231029.055000628.ACTUALSYSLOADWZNP6345_xml.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=954319215'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231028.055001255.ACTUALSYSLOADWZNP6345_csv.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=954089010'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231028.055001038.ACTUALSYSLOADWZNP6345_xml.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=954089006'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231027.055001270.ACTUALSYSLOADWZNP6345_csv.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=953844845'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231027.055001223.ACTUALSYSLOADWZNP6345_xml.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=953844844'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231026.055001417.ACTUALSYSLOADWZNP6345_csv.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=953598193'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231026.055001372.ACTUALSYSLOADWZNP6345_xml.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=953598192'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231025.055000904.ACTUALSYSLOADWZNP6345_csv.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=953360302'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231025.055000793.ACTUALSYSLOADWZNP6345_xml.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=953360051'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231024.055000801.ACTUALSYSLOADWZNP6345_csv.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=953120194'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231024.055000749.ACTUALSYSLOADWZNP6345_xml.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=953120023'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231023.055001053.ACTUALSYSLOADWZNP6345_csv.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=952868098'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231023.055001002.ACTUALSYSLOADWZNP6345_xml.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=952868396'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231022.055000687.ACTUALSYSLOADWZNP6345_csv.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=952639653'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231022.055000649.ACTUALSYSLOADWZNP6345_xml.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=952639652'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231021.055000819.ACTUALSYSLOADWZNP6345_csv.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=952411921'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231021.055000773.ACTUALSYSLOADWZNP6345_xml.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=952411920'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231020.055000484.ACTUALSYSLOADWZNP6345_csv.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=952173876'>zip</a></div></td></tr><tr
 ><td class='labelOptional_ind'>cdr.00013101.0000000000000000.20231020.055000433.ACTUALSYSLOADWZNP6345_xml.zip</td
 ><td  class='labelOptional'><div align='center'></div></td><td  class='labelOptional'><div 
 align='center'></div></td><td  class='labelOptional'><div align='center'><a 
 href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=952174057'>zip</a></div></td></tr
 ></table></td></tr></table>"""


urls_content_forecast = """<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 
Transitional//EN""http://www.w3.org/TR/html4/loose.dtd"><html><head><title>ERCOT MIS</title><meta 
http-equiv="Content-Type" content="text/html"; charset="utf-8"><meta http-equiv="pragma" content="no-cache"><meta 
http-equiv="cache-control" content="no-cache"><meta http-equiv="expires" content="0"><base 
href="https://mis.ercot.com:443/misapp/"><link href="/misapp/css/vdg.css" rel="stylesheet" type="text/css"><script 
language="javascript" src="/misapp/scripts/portletTypeA.js"></script></head><body><form name="PortletTypeAForm" 
method="GET" action="/misapp/GetReports.do;jsessionid=121A9F26DDBEA32D77CB6D1A8FAC7DC7.w_prblwbc0012b_e2"> <input 
type="hidden" name="basePath" id ="basePath" value="https://mis.ercot.com:443/misapp/"/><table width='800' border='0' 
cellpadding='0' cellspacing='0' class='tbl_ltTaupe'><tr><td class='hdr_grey'><table width='100%' border='0' 
cellspacing='0' cellpadding='0><tr class='hdr_grey'><td><b>Seven-Day Load Forecast by Weather Zone</b></td><td><div 
align='right'></div></td></tr></table></td></tr><tr><td><table width='800' border='0' cellspacing='0' 
cellpadding='4'><tr><td class='labelRequired_ind'><b>Title</b></td><td class='labelRequired'><div 
align='center'><b>XML</b></div></td><td class='labelRequired'><div align='center'><b>CSV</b></div></td><td 
class='labelRequired'><div align='center'><b>Other</b></div></td></tr><tr><td 
class='labelOptional_ind'>cdr.00012312.0000000000000000.20231121.113000947.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959878226'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231121.113000870.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959879096'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231121.103000844.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959869286'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231121.103000795.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959869028'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231121.093000715.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959854028'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231121.093000620.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959853862'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231121.083000978.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959841421'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231121.083000647.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959841417'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231121.073000714.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959830136'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231121.073000638.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959830473'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231121.063000996.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959819674'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231121.063000933.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959819673'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231121.053000991.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959807914'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231121.053000916.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959808260'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231121.043000722.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959795147'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231121.043000660.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959795478'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231121.033000594.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959786655'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231121.033000524.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959786319'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231121.023000647.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959777613'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231121.023000579.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959777612'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231121.013000671.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959768789'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231121.013000605.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959768788'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231121.003002949.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959759987'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231121.003002883.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959759986'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.233000886.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959750360'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.233000832.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959750358'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.223000746.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959740639'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.223000674.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959740638'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.213000634.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959731895'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.213000555.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959731893'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.203003862.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959722311'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.203003749.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959722137'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.193003016.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959711744'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.193002932.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959711976'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.183000767.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959700662'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.183000704.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959700508'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.173000910.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959691127'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.173000818.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959691363'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.163000873.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959680499'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.163000803.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959680317'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.153000703.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959670123'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.153000639.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959670122'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.143000805.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959659051'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.143000717.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959658846'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.133000876.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959648404'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.133000799.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959648329'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.123000704.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959632937'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.123000636.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959632990'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.113000937.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959623986'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.113000860.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959623985'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.103000931.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959614744'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.103000829.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959614743'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.093000786.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959604311'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.093000711.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959604308'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.083000958.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959591657'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.083000887.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959591333'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.073000657.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959580752'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.073000564.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959580526'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.063000700.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959570382'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.063000612.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959570381'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.053000867.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959558380'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.053000763.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959558379'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.043001060.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959545577'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.043000971.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959545576'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.033000790.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959536773'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.033000733.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959536513'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.023003151.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959527710'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.023003021.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959527524'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.013004958.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959518824'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.013004846.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959518822'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.003000884.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959510065'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231120.003000805.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959510064'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.233000754.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959500285'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.233000680.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959500284'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.223000763.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959490605'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.223000687.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959490604'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.213000937.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959481826'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.213000883.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959481825'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.203002843.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959473178'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.203002748.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959473177'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.193001076.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959464238'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.193000972.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959464237'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.183004389.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959455573'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.183004280.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959455283'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.173000833.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959446044'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.173000741.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959446288'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.163000807.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959437315'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.163000693.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959437395'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.153002884.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959428525'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.153002766.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959428598'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.143001054.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959417681'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.143000982.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959417680'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.133000741.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959408994'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.133000677.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959408826'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.123001145.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959399250'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.123000986.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959399365'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.113000994.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959390373'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.113000923.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959390231'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.103000854.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959381465'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.103000794.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959381464'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.093000799.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959370816'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.093000713.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959370992'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.083003124.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959358418'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.083003023.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959358589'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.073000801.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959347740'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.073000684.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959347738'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.063000823.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959337143'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.063000746.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959337666'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.053000703.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959325948'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.053000625.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959325673'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.043000822.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959313274'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.043000746.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959313270'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.033002584.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959304668'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.033002524.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959304667'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.023001118.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959295694'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.023001048.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959295693'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.013002886.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959287231'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.013002823.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959287230'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.003001271.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959276116'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231119.003001202.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959278372'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.233001692.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959268877'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.233001611.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959268530'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.223003232.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959259279'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.223003115.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959259276'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.213003531.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959250145'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.213003251.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959250143'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.203001419.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959241536'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.203001328.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959241535'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.193001023.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959232890'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.193000949.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959232889'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.183000805.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959224006'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.183000735.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959224182'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.173000807.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959214786'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.173000712.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959214785'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.163000824.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959205969'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.163000757.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959205848'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.153001492.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959197115'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.153001406.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959197114'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.143000792.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959185850'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.143000724.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959185849'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.133002938.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959177052'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.133002874.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959177049'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.123001841.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959167187'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.123001776.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959166944'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.113001554.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959158229'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.113001463.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959158372'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.103000596.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959149382'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.103000528.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959149381'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.093003526.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959139000'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.093003472.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959138998'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.083000810.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959126321'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.083000756.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959126319'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.073001590.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959115587'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.073001464.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959115586'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.063000926.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959105683'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.063000849.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959105681'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.053001065.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959093764'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.053000992.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959093840'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.043000916.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959081239'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.043000836.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959081237'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.033003455.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959072572'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.033003403.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959072571'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.023000841.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959063638'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.023000773.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959063637'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.013003392.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959054891'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.013003286.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959054890'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.003000791.LFCWEATHERNP3561_csv.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959045477'>zip</a></div></td></tr><tr
><td class='labelOptional_ind'>cdr.00012312.0000000000000000.20231118.003000726.LFCWEATHERNP3561_xml.zip</td><td 
class='labelOptional'><div align='center'></div></td><td class='labelOptional'><div align='center'></div></td><td 
class='labelOptional'><div align='center'><a 
href='/misdownload/servlets/mirDownload?mimic_duns=1118490502000&doclookupId=959045476'>zip</a></div></td></tr
></table></td></tr></table>"""


def test_ercot_daily_load_iso_read_setup(spark_session: SparkSession):
    iso_source = ERCOTDailyLoadISOSource(spark_session, iso_configuration)

    assert iso_source.system_type().value == 2
    assert iso_source.libraries() == Libraries(
        maven_libraries=[], pypi_libraries=[], pythonwheel_libraries=[]
    )

    assert isinstance(iso_source.settings(), dict)
    expected_values = [
        "certificate_pfx_key",
        "certificate_pfx_key_contents",
        "date",
        "load_type",
    ]
    assert sorted(iso_source.required_options) == expected_values
    assert iso_source.pre_read_validation()


def mock_certificates(mocker):
    class Key:
        @staticmethod
        def private_bytes(*args, **kwargs) -> bytes:
            return b""

    class Cert:
        @staticmethod
        def public_bytes(*args, **kwargs) -> bytes:
            return b""

    mocker.patch(
        "cryptography.hazmat.primitives.serialization.pkcs12.load_key_and_certificates",
        side_effect=lambda *args, **kwargs: (Key(), Cert(), ""),
    )


def ercot_daily_load_iso_read_batch_test(
    spark_session: SparkSession,
    mocker: MockerFixture,
    load_type: str,
    url_to_match: str,
    file_download_url_to_match: str,
    urls_bytes: bytes,
    zip_file: str,
    date: str,
    expected_data_file: str,
    expected_rows_count: int = 24,
):
    iso_source = ERCOTDailyLoadISOSource(
        spark_session,
        {**iso_configuration, "load_type": load_type, "date": date},
    )

    with open(f"{dir_path}/{zip_file}", "rb") as file:
        zip_download_bytes = file.read()

    class URLsResponse:
        content = urls_bytes
        status_code = 200

    class ZipDownloadResponse(URLsResponse):
        content = zip_download_bytes

    def get_response(url: str, *args, **kwargs):
        if url == url_to_match:
            return URLsResponse()
        else:
            assert url == file_download_url_to_match
            return ZipDownloadResponse()

    mocker.patch(patch_module_name, side_effect=get_response)
    mock_certificates(mocker)

    df = iso_source.read_batch()

    assert df.count() == expected_rows_count
    assert isinstance(df, DataFrame)
    assert str(df.schema) == str(ERCOT_SCHEMA)

    expected_df_spark = spark_session.createDataFrame(
        pd.read_csv(f"{dir_path}/{expected_data_file}", parse_dates=["Date"]),
        schema=ERCOT_SCHEMA,
    )

    cols = df.columns
    assert df.orderBy(cols).collect() == expected_df_spark.orderBy(cols).collect()


def test_ercot_daily_load_iso_read_batch_actual(
    spark_session: SparkSession, mocker: MockerFixture
):
    ercot_daily_load_iso_read_batch_test(
        spark_session,
        mocker,
        load_type="actual",
        url_to_match=url_actual,
        file_download_url_to_match=(
            "https://mis.ercot.com/misdownload/servlets/mirDownload?mimic_duns=1118490502000"
            "&doclookupId=959096531"
        ),
        urls_bytes=urls_content_actual.encode("utf-8"),
        zip_file="ercot_daily_load_actual_sample1.zip",
        expected_data_file="ercot_daily_load_actual_expected.csv",
        date="2023-11-17",
    )


def test_ercot_daily_load_iso_read_batch_forecast(
    spark_session: SparkSession, mocker: MockerFixture
):
    ercot_daily_load_iso_read_batch_test(
        spark_session,
        mocker,
        load_type="forecast",
        url_to_match="https://mis.ercot.com/misapp/GetReports.do?reportTypeId=12312",
        file_download_url_to_match=(
            "https://mis.ercot.com/misdownload/servlets/mirDownload?mimic_duns=1118490502000"
            "&doclookupId=959500285"
        ),
        urls_bytes=urls_content_forecast.encode("utf-8"),
        zip_file="ercot_daily_load_forecast_sample1.zip",
        date="2023-11-19",
        expected_data_file="ercot_daily_load_forecast_expected.csv",
        expected_rows_count=192,
    )


def test_ercot_daily_load_iso_empty_response(
    spark_session: SparkSession, mocker: MockerFixture
):
    iso_source = ERCOTDailyLoadISOSource(
        spark_session,
        {**iso_configuration, "load_type": "actual", "date": "2023-11-16"},
    )

    class EmptyURLsResponse:
        content = urls_content_actual
        status_code = 200

    class EmptyZipDownloadResponse(EmptyURLsResponse):
        content = b""

    def get_response(url: str, *args, **kwargs):
        if url == url_actual:
            return EmptyURLsResponse()
        else:
            return EmptyZipDownloadResponse()

    mocker.patch(patch_module_name, side_effect=get_response)
    mock_certificates(mocker)

    with pytest.raises(HTTPError) as exc_info:
        iso_source.read_batch()

    assert str(exc_info.value) == "Empty Response was returned"


def test_ercot_daily_load_iso_no_file(
    spark_session: SparkSession, mocker: MockerFixture
):
    iso_source = ERCOTDailyLoadISOSource(
        spark_session,
        {**iso_configuration, "date": "2023-09-16"},
    )

    class NoFileURLsResponse:
        content = urls_content_actual
        status_code = 200

    def get_response(url: str, *args, **kwargs):
        if url == url_actual:
            return NoFileURLsResponse()

    mocker.patch(patch_module_name, side_effect=get_response)
    mock_certificates(mocker)

    with pytest.raises(ValueError) as exc_info:
        iso_source.read_batch()

    assert str(exc_info.value) == "No file was found for date - 20230917"


def test_ercot_daily_load_iso_no_data(
    spark_session: SparkSession, mocker: MockerFixture
):
    iso_source = ERCOTDailyLoadISOSource(
        spark_session,
        {**iso_configuration},
    )

    with open(f"{dir_path}/ercot_daily_load_actual_sample2.zip", "rb") as file:
        no_data_zip_download_bytes = file.read()

    class NoDataURLsResponse:
        content = urls_content_actual
        status_code = 200

    class NoDataZipDownloadResponse(NoDataURLsResponse):
        content = no_data_zip_download_bytes

    def get_response(url: str, *args, **kwargs):
        if url == url_actual:
            return NoDataURLsResponse()
        else:
            return NoDataZipDownloadResponse()

    mocker.patch(patch_module_name, side_effect=get_response)
    mock_certificates(mocker)

    with pytest.raises(ValueError) as exc_info:
        iso_source.read_batch()

    assert str(exc_info.value) == "No data was found in the specified interval"


def test_ercot_daily_load_iso_iso_invalid_date(spark_session: SparkSession):
    with pytest.raises(ValueError) as exc_info:
        iso_source = ERCOTDailyLoadISOSource(
            spark_session, {**iso_configuration, "date": "2023/11/01"}
        )
        iso_source.pre_read_validation()

    expected = "Unable to parse date. Please specify in %Y-%m-%d format."
    assert str(exc_info.value) == expected
