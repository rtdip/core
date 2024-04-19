# Spark Connect

[Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) was released in Apache Spark 3.4.0 to enable a decoupled client-server architecture that allows remote connectivity to Spark clusters using the Spark DataFrame API.

This means any Spark cluster could provide compute to a spark job and therefore enables options such as Spark on Kubernetes, Spark running locally or Databricks Interactive Clusters to be leveraged in the RTDIP SDK to perform time series queries.

## Prerequisites

Please ensure that you have followed the [instructions](https://spark.apache.org/docs/latest/spark-connect-overview.html#how-to-use-spark-connect) to enable Spark Connect on your Spark cluster and that you are using a `pyspark>=3.4.0`. If you are connecting to Databricks, then install `databricks-connect>=13.0.1` instead of `pyspark`.

## Example

Below is an example of connecting to Spark using Spark Connect.

```python
from rtdip_sdk.connectors import SparkConnection

spark_server = "sparkserver.com"
access_token = "my_token"

spark_remote = "sc://{}:443;token={}".format(spark_server, access_token)
connection = SparkConnection(spark_remote=spark_remote)
```

Replace the **access_token** with your own information(this assumes an access token is required to authenticate with the remote Spark server).
