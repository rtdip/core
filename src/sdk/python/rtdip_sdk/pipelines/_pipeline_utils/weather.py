# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType
# CLASS,EXPIRE_TIME_GMT,FCST_VALID,FCST_VALID_LOCAL,NUM,\
# DAY_IND,TEMP,DEWPT,HI,WC,FEELS_LIKE,ICON_EXTD,WXMAN,\
# ICON_CODE,DOW,PHRASE_12CHAR,PHRASE_22CHAR,PHRASE_32CHAR,\
# SUBPHRASE_PT1,SUBPHRASE_PT2,SUBPHRASE_PT3,POP,PRECIP_TYPE,QPF,SNOW_QPF,RH,WSPD,WDIR,WDIR_CARDINAL,\
# GUST,CLDS,VIS,MSLP,UV_INDEX_RAW,UV_INDEX,UV_WARNING,UV_DESC,GOLF_INDEX,GOLF_CATEGORY,SEVERITY,LATITUDE,LONGITUDE
WEATHER_FORECAST_SCHEMA = StructType(
    [
        StructField("CLASS", StringType(), True),
        StructField("EXPIRE_TIME_GMT", IntegerType(), True),
        StructField("FCST_VALID", IntegerType(), True),
        StructField("FCST_VALID_LOCAL", StringType(), True),
        StructField("NUM", IntegerType(), True),
        StructField("DAY_IND", StringType(), True),
        StructField("TEMP", IntegerType(), True),
        StructField("DEWPT", IntegerType(), True),
        StructField("HI", IntegerType(), True),
        StructField("WC", IntegerType(), True),
        StructField("FEELS_LIKE", IntegerType(), True),
        StructField("ICON_EXTD", IntegerType(), True),
        StructField("WXMAN", StringType(), True),
        StructField("ICON_CODE", IntegerType(), True),
        StructField("DOW", StringType(), True),
        StructField("PHRASE_12CHAR", StringType(), True),
        StructField("PHRASE_22CHAR", StringType(), True),
        StructField("PHRASE_32CHAR", StringType(), True),
        StructField("SUBPHRASE_PT1", StringType(), True),
        StructField("SUBPHRASE_PT2", StringType(), True),
        StructField("SUBPHRASE_PT3", StringType(), True),
        StructField("POP", StringType(), True),
        StructField("PRECIP_TYPE", StringType(), True),
        StructField("QPF", DoubleType(), True),
        StructField("SNOW_QPF", DoubleType(), True),
        StructField("RH", IntegerType(), True),
        StructField("WSPD", IntegerType(), True),
        StructField("WDIR", IntegerType(), True),
        StructField("WDIR_CARDINAL", StringType(), True),
        StructField("GUST", DoubleType(), True),
        StructField("CLDS", IntegerType(), True),
        StructField("VIS", DoubleType(), True),
        StructField("MSLP", DoubleType(), True),
        StructField("UV_INDEX_RAW", DoubleType(), True),
        StructField("UV_INDEX", IntegerType(), True),
        StructField("UV_WARNING", IntegerType(), True),
        StructField("UV_DESC", StringType(), True),
        StructField("GOLF_INDEX", DoubleType(), True),
        StructField("GOLF_CATEGORY", StringType(), True),
        StructField("SEVERITY", IntegerType(), True),
        StructField("LATITUDE", DoubleType(), True),
        StructField("LONGITUDE", DoubleType(), True),
    ]
)

# WEATHER_FORECAST_MULTI_SCHEMA = StructType(
#     [
#         StructField("LATITUDE", DoubleType(), True),
#         StructField("LONGITUDE", DoubleType(), True),
#         *WEATHER_FORECAST_SCHEMA.fields
#     ]
# )



WEATHER_DATA_MODEL = StructType(
    [
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
          # StructField('weather_id', StringType(),  False),
          StructField('weather_day', StringType(), False),
          StructField('weather_hour', IntegerType(), False),
          StructField('weather_timezone_offset', StringType(), False),
          StructField('weather_type', StringType(), False),
          StructField('processed_date', StringType(), False),
          StructField('temperature', DoubleType(), True),
          StructField('dew_point', DoubleType(), True),
          StructField('humidity', DoubleType(), True),
          StructField('heat_index', DoubleType(), True),
          StructField('wind_chill', DoubleType(), True),
          StructField('wind_direction', DoubleType(), True),
          StructField('wind_speed', DoubleType(), True),
          StructField('cloud_cover', DoubleType(), True),
          StructField('wet_bulb_temp', StringType(), True),
          StructField('solar_irradiance', StringType(), True),
          StructField('precipitation', DoubleType(), True),
          StructField('day_or_night', StringType(), True),
          StructField('day_of_week', StringType(), True),
          StructField('wind_gust', IntegerType(), True),
          StructField('msl_pressure', DoubleType(), True),
          StructField('forecast_day_num', IntegerType(), True),
          StructField('prop_of_precip', IntegerType(), True),
          StructField('precip_type', StringType(), True),
          StructField('snow_accumulation', DoubleType(), True),
          StructField('uv_index', DoubleType(), True),
          StructField('visibility', DoubleType(), True)
    ]
)
