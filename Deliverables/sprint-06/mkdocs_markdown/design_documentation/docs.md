# Design Documentation

The RTDIP pipeline SDK allows you to ingest data in batches from various sources, transform it, and send the data to your desired destination.
The SDK uses [PySpark](https://spark.apache.org/docs/latest/api/python/index.html) internally to process data, and provides a modular interface that allows you to build your pipeline, by using the provided components in any desired sequence.

## Ingestion

The SDK provides ingestion components that facilitate the integration of data from various sources, such as industrial IoT systems, databases, and streaming platforms. These components abstract complexities, standardizing data into PySpark [DataFrames](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html), which act as the primary data structure for subsequent stages.

## Transform

Transformation components allow users to modify, clean, and enrich ingested data. They are designed to be composable and configurable, enabling tasks such as data validation, format normalization, or feature engineering.

## Monitoring

Monitoring components analyze DataFrames to provide insights into the data quality. You have to provide them with a Logger, from which you can extract the results of the monitoring.

## Destination

The RTDIP SDK provides pipeline components to write your processed data to sink/destination systems.
