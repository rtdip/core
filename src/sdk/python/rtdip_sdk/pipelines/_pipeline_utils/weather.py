# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, TimestampType

WEATHER_FORECAST_SCHEMA = StructType(
    [
        StructField("Latitude", DoubleType(), True),
        StructField("Longitude", DoubleType(), True),
        StructField("Class", StringType(), True),
        StructField("ExpireTimeGmt", IntegerType(), True),
        StructField("FcstValid", IntegerType(), True),
        StructField("FcstValidLocal", StringType(), True),
        StructField("Num", IntegerType(), True),
        StructField("DayInd", StringType(), True),
        StructField("Temp", IntegerType(), True),
        StructField("Dewpt", IntegerType(), True),
        StructField("Hi", IntegerType(), True),
        StructField("Wc", IntegerType(), True),
        StructField("FeelsLike", IntegerType(), True),
        StructField("IconExtd", IntegerType(), True),
        StructField("Wxman", StringType(), True),
        StructField("IconCode", IntegerType(), True),
        StructField("Dow", StringType(), True),
        StructField("Phrase12Char", StringType(), True),
        StructField("Phrase22Char", StringType(), True),
        StructField("Phrase32Char", StringType(), True),
        StructField("SubphrasePt1", StringType(), True),
        StructField("SubphrasePt2", StringType(), True),
        StructField("SubphrasePt3", StringType(), True),
        StructField("Pop", StringType(), True),
        StructField("PrecipType", StringType(), True),
        StructField("Qpf", DoubleType(), True),
        StructField("SnowQpf", DoubleType(), True),
        StructField("Rh", IntegerType(), True),
        StructField("Wspd", IntegerType(), True),
        StructField("Wdir", IntegerType(), True),
        StructField("WdirCardinal", StringType(), True),
        StructField("Gust", DoubleType(), True),
        StructField("Clds", IntegerType(), True),
        StructField("Vis", DoubleType(), True),
        StructField("Mslp", DoubleType(), True),
        StructField("UvIndexRaw", DoubleType(), True),
        StructField("UvIndex", IntegerType(), True),
        StructField("UvWarning", IntegerType(), True),
        StructField("UvDesc", StringType(), True),
        StructField("GolfIndex", DoubleType(), True),
        StructField("GolfCategory", StringType(), True),
        StructField("Severity", IntegerType(), True),

    ]
)

WEATHER_DATA_MODEL = StructType(
    [
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
        StructField('weather_day', StringType(), False),
        StructField('weather_hour', IntegerType(), False),
        StructField('weather_timezone_offset', StringType(), False),
        StructField('weather_type', StringType(), False),
        StructField('processed_date', TimestampType(), False),
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
