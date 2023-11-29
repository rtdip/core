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
    </b>RTDIP are continuously adding more to this list. For detailed information on timelines, read this [blog post](../../blog/posts/rtdip_ingestion_pipelines.md) and check back on this page regularly.<br />

### Sources

Sources are components that connect to source systems and extract data from them. These will typically be real time data sources, but also support batch components as these are still important and necessary data souces of time series data in a number of circumstances in the real world.

|Source Type|Python|Apache Spark|Databricks|Azure|AWS|
|---------------------------|----------------------|--------------------|----------------------|----------------------|---------|
|[Delta](../code-reference/pipelines/sources/spark/delta.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Delta Sharing](../code-reference/pipelines/sources/spark/delta_sharing.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Autoloader](../code-reference/pipelines/sources/spark/autoloader.md)|||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Eventhub](../code-reference/pipelines/sources/spark/eventhub.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Eventhub Kafka](../code-reference/pipelines/sources/spark/kafka_eventhub.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[IoT Hub](../code-reference/pipelines/sources/spark/iot_hub.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Kafka](../code-reference/pipelines/sources/spark/kafka.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Kinesis](../code-reference/pipelines/sources/spark/kafka.md)|||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[MISO Daily Load ISO](../code-reference/pipelines/sources/spark/iso/miso_daily_load_iso.md)  ||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[MISO Historical Load ISO](../code-reference/pipelines/sources/spark/iso/miso_historical_load_iso.md) ||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[PJM Daily Load ISO](../code-reference/pipelines/sources/spark/iso/pjm_daily_load_iso.md)  ||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[PJM Historical Load ISO](../code-reference/pipelines/sources/spark/iso/pjm_historical_load_iso.md) ||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[CAISO Daily Load ISO](../code-reference/pipelines/sources/spark/iso/caiso_daily_load_iso.md)  ||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[CAISO Historical Load ISO](../code-reference/pipelines/sources/spark/iso/caiso_historical_load_iso.md) ||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Weather Forecast API V1](../code-reference/pipelines/sources/spark/the_weather_company/weather_forecast_api_v1.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Weather Forecast API V1 Multi](../code-reference/pipelines/sources/spark/the_weather_company/weather_forecast_api_v1_multi.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
!!! note "Note"
    This list will dynamically change as the framework is further developed and new components are added.

### Transformers

Transformers are components that perform transformations on data. These will target certain data models and common transformations that sources or destination components require to be performed on data before it can be ingested or consumed.

|Transformer Type|Python|Apache Spark|Databricks|Azure|AWS|
|---------------------------|----------------------|--------------------|----------------------|----------------------|---------|
|[Binary To String](../code-reference/pipelines/transformers/spark/binary_to_string.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[OPC Publisher OPCUA Json To Process Control Data Model](../code-reference/pipelines/transformers/spark/opc_publisher_opcua_json_to_pcdm.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Fledge OPCUA Json To Process Control Data Model](../code-reference/pipelines/transformers/spark/fledge_opcua_json_to_pcdm.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[EdgeX OPCUA Json To Process Control Data Model](../code-reference/pipelines/transformers/spark/edgex_opcua_json_to_pcdm.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[SSIP PI Binary Files To Process Control Data Model](../code-reference/pipelines/transformers/spark/ssip_pi_binary_file_to_pcdm.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[SSIP PI Binary JSON To Process Control Data Model](../code-reference/pipelines/transformers/spark/ssip_pi_binary_json_to_pcdm.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[MISO To Meters Data Model](../code-reference/pipelines/transformers/spark/iso/miso_to_mdm.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Pandas to PySpark DataFrame Conversion](../code-reference/pipelines/transformers/spark/pandas_to_pyspark.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[PySpark to Pandas DataFrame Conversion](../code-reference/pipelines/transformers/spark/pyspark_to_pandas.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[MISO To Meters Data Model](../code-reference/pipelines/transformers/spark/iso/miso_to_mdm.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Raw Forecast to Weather Data Model](../code-reference/pipelines/transformers/spark/the_weather_company/raw_forecast_to_weather_data_model.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[PJM To Meters Data Model](../code-reference/pipelines/transformers/spark/iso/pjm_to_mdm.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|

!!! note "Note"
    This list will dynamically change as the framework is further developed and new components are added.

### Destinations

Destinations are components that connect to sink/destination systems and write data to them. 

|Destination Type|Python|Apache Spark|Databricks|Azure|AWS|
|---------------------------|----------------------|--------------------|----------------------|----------------------|---------|
|[Delta](../code-reference/pipelines/destinations/spark/delta.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Delta Merge](../code-reference/pipelines/destinations/spark/delta_merge.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Eventhub](../code-reference/pipelines/destinations/spark/eventhub.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Kakfa](../code-reference/pipelines/destinations/spark/kafka.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Kinesis](../code-reference/pipelines/destinations/spark/kafka.md)|||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Rest API](../code-reference/pipelines/destinations/spark/rest_api.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Process Control Data Model To Delta](../code-reference/pipelines/destinations/spark/pcdm_to_delta.md)|||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Process Control Data Model Latest Values To Delta](../code-reference/pipelines/destinations/spark/pcdm_latest_to_delta.md)|||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[EVM](../code-reference/pipelines/destinations/blockchain/evm.md)|:heavy_check_mark:|||:heavy_check_mark:|:heavy_check_mark:|

!!! note "Note"
    This list will dynamically change as the framework is further developed and new components are added.

### Utilities

Utilities are components that perform utility functions such as logging, error handling, data object creation, authentication, maintenance and are normally components that can be executed as part of a pipeline or standalone.

|Utility Type|Python|Apache Spark|Databricks|Azure|AWS|
|---------------------------|----------------------|--------------------|----------------------|----------------------|---------|
|[Spark Session](../code-reference/pipelines/utilities/spark/session.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Spark Configuration](../code-reference/pipelines/utilities/spark/configuration.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Delta Table Create](../code-reference/pipelines/utilities/spark/delta_table_create.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Delta Table Optimize](../code-reference/pipelines/utilities/spark/delta_table_optimize.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Delta Table Vacuum](../code-reference/pipelines/utilities/spark/delta_table_vacuum.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[AWS S3 Bucket Policy](../code-reference/pipelines/utilities/aws/s3_bucket_policy.md)|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[ADLS Gen 2 ACLs](../code-reference/pipelines/utilities/azure/adls_gen2_acl.md)|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Azure Autoloader Resources](../code-reference/pipelines/utilities/azure/autoloader_resources.md)|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Spark ADLS Gen 2 Service Principal Connect](../code-reference/pipelines/utilities/spark/adls_gen2_spn_connect.md)||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|

!!! note "Note"
    This list will dynamically change as the framework is further developed and new components are added.

### Secrets

Secrets are components that perform functions to interact with secret stores to manage sensitive information such as passwords, keys and certificates.

|Secret Type|Python|Apache Spark|Databricks|Azure|AWS|
|---------------------------|----------------------|--------------------|----------------------|----------------------|---------|
|[Databricks Secret Scopes](../code-reference/pipelines/secrets/databricks.md)|||:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Hashicorp Vault](../code-reference/pipelines/secrets/hashicorp_vault.md)|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|
|[Azure Key Vault](../code-reference/pipelines/secrets/azure_key_vault.md)|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|:heavy_check_mark:|

!!! note "Note"
    This list will dynamically change as the framework is further developed and new components are added.

## Conclusion

Components can be used to build RTDIP Pipelines which is described in more detail [here.](jobs.md)