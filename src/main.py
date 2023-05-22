from pyspark.sql import SparkSession

from rtdip_sdk.pipelines.execute import PipelineJob, PipelineStep, PipelineTask
from rtdip_sdk.pipelines.sources import SparkEventhubSource
from rtdip_sdk.pipelines.transformers import EventhubBodyBinaryToString
from rtdip_sdk.pipelines.destinations import SparkDeltaDestination
from rtdip_sdk.pipelines.secrets import PipelineSecret, DatabricksSecrets



# from sdk.python.rtdip_sdk.pipelines.execute.models import PipelineJob, PipelineStep, PipelineTask
# from sdk.python.rtdip_sdk.pipelines.sources import SparkEventhubSource
# from sdk.python.rtdip_sdk.pipelines.transformers import EventhubBodyBinaryToString
# from sdk.python.rtdip_sdk.pipelines.destinations import SparkDeltaDestination
# from sdk.python.rtdip_sdk.pipelines.secrets import PipelineSecret, DatabricksSecrets

print("hello world") 
eventhub_configuration = {
    "eventhubs.connectionString": PipelineSecret(type=DatabricksSecrets, vault="test_vault", key="test_key"),
    "eventhubs.consumerGroup": "$Default",
    "eventhubs.startingPosition": {"offset": "0", "seqNo": -1, "enqueuedTime": None, "isInclusive": True}
}   

# p = PipelineJob()
# print(p)
print("end")