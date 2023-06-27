# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType

WEATHER_FORECAST_SCHEMA = StructType(
    [
        StructField("CLASS", StringType(), True),
        StructField("CLDS", IntegerType(), True),
        StructField("DAY_IND", StringType(), True),
        StructField("DEWPT", IntegerType(), True),
        StructField("DOW", StringType(), True),
        StructField("EXPIRE_TIME_GMT", IntegerType(), True),
        StructField("FCST_VALID", IntegerType(), True),
        StructField("FCST_VALID_LOCAL", StringType(), True),
        StructField("FEELS_LIKE", IntegerType(), True),
        StructField("GOLF_CATEGORY", StringType(), True),
        StructField("GOLF_INDEX", DoubleType(), True),
        StructField("GUST", DoubleType(), True),
        StructField("HI", IntegerType(), True),
        StructField("ICON_CODE", IntegerType(), True),
        StructField("ICON_EXTD", IntegerType(), True),
        StructField("MSLP", DoubleType(), True),
        StructField("NUM", IntegerType(), True),
        StructField("PHRASE_12CHAR", StringType(), True),
        StructField("PHRASE_22CHAR", StringType(), True),
        StructField("PHRASE_32CHAR", StringType(), True),
        StructField("POP", StringType(), True),
        StructField("PRECIP_TYPE", StringType(), True),
        StructField("QPF", DoubleType(), True),
        StructField("RH", IntegerType(), True),
        StructField("SEVERITY", IntegerType(), True),
        StructField("SNOW_QPF", DoubleType(), True),
        StructField("SUBPHRASE_PT1", StringType(), True),
        StructField("SUBPHRASE_PT2", StringType(), True),
        StructField("SUBPHRASE_PT3", StringType(), True),
        StructField("TEMP", IntegerType(), True),
        StructField("UV_DESC", StringType(), True),
        StructField("UV_INDEX", IntegerType(), True),
        StructField("UV_INDEX_RAW", DoubleType(), True),
        StructField("UV_WARNING", IntegerType(), True),
        StructField("VIS", DoubleType(), True),
        StructField("WC", IntegerType(), True),
        StructField("WDIR", IntegerType(), True),
        StructField("WDIR_CARDINAL", StringType(), True),
        StructField("WSPD", IntegerType(), True),
        StructField("WXMAN", StringType(), True),
    ]
)

WEATHER_FORECAST_MULTI_SCHEMA = StructType(
    [
        StructField("LATITUDE", DoubleType(), True),
        StructField("LONGITUDE", DoubleType(), True),
        *WEATHER_FORECAST_SCHEMA.fields
    ]
)



COMM_FORECAST_SCHEMA = StructType(
    [
          StructField('weather_id', StringType(), False),
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
