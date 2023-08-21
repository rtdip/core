# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    StringType,
    IntegerType,
    TimestampType,
    FloatType,
    BooleanType,
)


RTDIP_FLOAT_WEATHER_DATA_MODEL = StructType(
    [
        StructField("TagName", StringType(), False),
        StructField("Longitude", DoubleType(), False),
        StructField("Latitude", DoubleType(), False),
        StructField("Longitude", DoubleType(), False),
        StructField("EventDate", TimestampType(), False),
        StructField("EventTime", TimestampType(), False),
        StructField("Source", StringType(), False),
        StructField("Status", StringType(), False),
        StructField("Value", FloatType(), False),
        StructField("EnqueuedTime", TimestampType(), True),
        StructField("Latest", BooleanType(), False),
    ]
)


RTDIP_STRING_WEATHER_DATA_MODEL = StructType(
    [
        StructField("TagName", StringType(), False),
        StructField("Longitude", DoubleType(), False),
        StructField("Latitude", DoubleType(), False),
        StructField("Longitude", DoubleType(), False),
        StructField("EventDate", TimestampType(), False),
        StructField("EventTime", TimestampType(), False),
        StructField("Source", StringType(), False),
        StructField("Status", StringType(), False),
        StructField("Value", StringType(), False),
        StructField("EnqueuedTime", TimestampType(), True),
        StructField("Latest", BooleanType(), False),
    ]
)
