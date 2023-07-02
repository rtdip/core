from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.spark import SparkClient
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries
from src.sdk.python.rtdip_sdk.pipelines.sources import SparkDeltaSource
from src.sdk.python.rtdip_sdk.pipelines.sources.interfaces import SourceInterface

import inspect
import sys

clsmembers = inspect.getmembers(sys.modules[__name__], inspect.isclass)
component_list = []
for cls in clsmembers:
   class_check = getattr(sys.modules[__name__], cls[0])
   if issubclass(class_check, SourceInterface) and class_check != SourceInterface:
      component_list.append(cls[1])
      print(cls[0])

task_libraries = Libraries()
task_libraries.get_libraries_from_components(component_list)
spark_configuration = {}
for component in component_list:
    spark_configuration = {**spark_configuration, **component.settings()}
spark = SparkClient(spark_configuration=spark_configuration, spark_libraries=task_libraries, spark_remote=None).spark_session
print(spark.version)