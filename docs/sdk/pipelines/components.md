# Pipeline Components

## Overview

The Real Time Data Ingestion Pipeline Framework supports the following component types:

- Sources - connectors to source systems
- Transformers - perform transformations on data, including data cleansing, data enrichment, data aggregation, data masking, data encryption, data decryption, data validation, data conversion, data normalization, data de-normalization, data partitioning etc
- Destinations - connectors to sink/destination systems 
- Utilities - components that perform utility functions such as logging, error handling, data object creation, authentication, maintenance etc
- Secrets - components that facilitate accessing secret stores where sensitive information is stored such as passwords, connectiong strings, keys etc

## Component Types

|Python|Apache Spark|Databricks|
|---------------------------|----------------------|--------------------------------------------------|
|![python](images/python.png)|![pyspark](images/apachespark.png)|![databricks](images/databricks_horizontal.png)|

Component Types determine system requirements to execute the component:

- Python - components that are written in python and can be executed on a python runtime
- Pyspark - components that are written in pyspark can be executed on an open source Apache Spark runtime
- Databricks - components that require a Databricks runtime

!!! note "Note"
    </b>RTDIP are continuously adding more to this list. For detailed information on timelines, read this [blog post](../../blog/rtdip_ingestion_pipelines.md) and check back on this page regularly<br />

### Sources

Sources are components that connect to source systems and extract data from them. These will typically be real time data sources, but also support batch components as these are still important and necessary data souces of time series data in a number of circumstances in the real world.

|Source Type|Python|Apache Spark|Databricks|Azure|AWS|
|---------------------------|----------------------|--------------------|----------------------|----------------------|---------|
|[Delta](../code-reference/pipelines/sources/spark/delta.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Delta Sharing](../code-reference/pipelines/sources/spark/delta_sharing.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Autoloader](../code-reference/pipelines/sources/spark/autoloader.md)|||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Eventhub](../code-reference/pipelines/sources/spark/eventhub.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:||
|[IoT Hub](../code-reference/pipelines/sources/spark/iot_hub.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:||
|[Kafka](../code-reference/pipelines/sources/spark/kafka.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Kinesis](../code-reference/pipelines/sources/spark/kafka.md)|||:heavy_check_mark:||:heavy_check_mark:|

!!! note "Note"
    This list will dynamically change as the framework is further developed and new components are added.

### Transformers

Transformers are components that perform transformations on data. These will target certain data models and common transformations that sources or destination components require to be performed on data before it can be ingested or consumed.

|Transformer Type|Python|Apache Spark|Databricks|Azure|AWS|
|---------------------------|----------------------|--------------------|----------------------|----------------------|---------|
|[Binary To String](../code-reference/pipelines/transformers/spark/binary_to_string.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Json To OPC UA](../code-reference/pipelines/transformers/spark/json_to_opcua.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[OPC UA To Process Control Data Model](../code-reference/pipelines/transformers/spark/opcua_to_process_control_data_model.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|

!!! note "Note"
    This list will dynamically change as the framework is further developed and new components are added.

### Destinations

Destinations are components that connect to sink/destination systems and write data to them. 

|Destination Type|Python|Apache Spark|Databricks|Azure|AWS|
|---------------------------|----------------------|--------------------|----------------------|----------------------|---------|
|[Delta Append](../code-reference/pipelines/destinations/spark/delta.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Eventhub](../code-reference/pipelines/destinations/spark/eventhub.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Kakfa](../code-reference/pipelines/destinations/spark/kafka.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Kinesis](../code-reference/pipelines/destinations/spark/kafka.md)|||:heavy_check_mark:||:heavy_check_mark:|

!!! note "Note"
    This list will dynamically change as the framework is further developed and new components are added.

### Utilities

Utilities are components that perform utility functions such as logging, error handling, data object creation, authentication, maintenance and are normally components that can be executed as part of a pipeline or standalone.

|Utility Type|Python|Apache Spark|Databricks|Azure|AWS|
|---------------------------|----------------------|--------------------|----------------------|----------------------|---------|
|[Spark Configuration](../code-reference/pipelines/utilities/spark/configuration.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Delta Table Create](../code-reference/pipelines/utilities/spark/delta_table_create.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Delta Table Optimize](../code-reference/pipelines/utilities/spark/delta_table_optimize.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Delta Table Vacuum](../code-reference/pipelines/utilities/spark/delta_table_vacuum.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[ADLS Gen 2 ACLs](../code-reference/pipelines/utilities/azure/adls_gen2_acl.md)|:heavy_check_mark:|||:heavy_check_mark:||

!!! note "Note"
    This list will dynamically change as the framework is further developed and new components are added.

### Secrets

Secrets are components that perform functions to interact with secret stores to manage sensitive information such as passwords, keys and certificates.

|Secret Type|Python|Apache Spark|Databricks|Azure|AWS|
|---------------------------|----------------------|--------------------|----------------------|----------------------|---------|
|[Databricks Secret Scopes](../code-reference/pipelines/secrets/databricks.md)|||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|

!!! note "Note"
    This list will dynamically change as the framework is further developed and new components are added.

## Conclusion

Components can be used to build RTDIP Pipelines which is described in more detail [here.](jobs.md)