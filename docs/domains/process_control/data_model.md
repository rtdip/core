# Process Control Data Model

The Process Control Data Model consists of two key components:

1. **Metadata** about the sensor/object/measurement such as Description, Unit of Measure, Status and also provides metadata used in queries such as Step logic used in interpolation.
2. **Events** contains transactional data and is simply capturing the name of the sensor/object/measurement, the timestamp of the event, the status of the event recording and the value.

## Data Model

``` mermaid
erDiagram
  METADATA ||--o{ EVENTS : contains
  METADATA {
    string TagName PK
    string Description
    string UoM
    string DataType
    boolean Step
    string Status
    dict Properties "Key Value pairs of varying metadata"
  }
  EVENTS {
    string TagName PK
    timestamp EventTime PK
    string Status
    dynamic Value "Value can be of different Data Types"
  }
```

## References

| Reference  | Description        |
|------------|--------------------|
|[IEC 61850](https://en.wikipedia.org/wiki/IEC_61850#:~:text=IEC%2061850%20is%20an%20international,architecture%20for%20electric%20power%20systems.)|Relevant description to IEC 61850|
|[IEC CIM](https://en.wikipedia.org/wiki/Common_Information_Model_(electricity))|Relevant description to IEC CIM|

## Mappings

### Fledge OPC UA South Plugin

[Fledge](https://www.lfedge.org/projects/fledge/){target=_blank} provides support for sending data between various data sources and data destinations. The mapping below is for the [OPC UA South Plugin](https://fledge-iot.readthedocs.io/en/latest/plugins/fledge-south-opcua/index.html){target=_blank} that can be sent to message brokers like Kafka, Azure IoT Hub etc.

This mapping is performed by the [RTDIP Fledge to PCDM Component](../../sdk/code-reference/pipelines/transformers/spark/fledge_opcua_json_to_pcdm.md) and can be used in an [RTDIP Ingestion Pipeline.](../../sdk/pipelines/framework.md)

| From Data Model | From Field | From Type | To Data Model |To Field| To Type | Mapping Logic |
|------|----|---------|------|------|--------|-----------|
| Fledge OPC UA | Object ID | string | EVENTS| TagName | string | |
| Fledge OPC UA | EventTime | string | EVENTS| EventTime | timestamp | Converted to a timestamp |
| | | | EVENTS| Status | string | Can be defaulted in [RTDIP Fledge to PCDM Component](../../sdk/code-reference/pipelines/transformers/spark/fledge_opcua_json_to_pcdm.md) otherwise Null |
| Fledge OPC UA | Value | string | EVENTS | Value | dynamic | Converts Value into either a float number or string based on how it is received in the message |

### OPC Publisher

[OPC Publisher](https://learn.microsoft.com/en-us/azure/industrial-iot/overview-what-is-opc-publisher){target=_blank} connects to OPC UA assets and publishes data to the Microsoft Azure Cloud's IoT Hub.

The mapping below is performed by the [RTDIP OPC Publisher to PCDM Component](../../sdk/code-reference/pipelines/transformers/spark/opc_publisher_opcua_json_to_pcdm.md) and can be used in an [RTDIP Ingestion Pipeline.](../../sdk/pipelines/framework.md)

| From Data Model | From Field | From Type | To Data Model |To Field| To Type | Mapping Logic |
|------|----|---------|------|------|--------|-----------|
| OPC Publisher | DisplayName | string | EVENTS| TagName | string | From Field can be specified in Component |
| OPC Publisher | SourceTimestamp | string | EVENTS| EventTime | timestamp | Converted to a timestamp |
| OPC Publisher | StatusCode.Symbol | string | EVENTS| Status | string | Null values can be overridden in the [RTDIP OPC Publisher to PCDM Component](../../sdk/code-reference/pipelines/transformers/spark/opc_publisher_opcua_json_to_pcdm.md) |
| OPC Publisher | Value.Value | string | EVENTS | Value | dynamic | Converts Value into either a float number or string based on how it is received in the message |

### EdgeX
[EdgeX](https://www.lfedge.org/projects/edgexfoundry/){target=_blank} provides support for sending data between various data sources and data destinations. 

This mapping is performed by the [RTDIP EdgeX to PCDM Component](../../sdk/code-reference/pipelines/transformers/spark/edgex_opcua_json_to_pcdm.md) and can be used in an [RTDIP Ingestion Pipeline.](../../sdk/pipelines/framework.md)

| From Data Model | From Field | From Type | To Data Model |To Field| To Type | Mapping Logic |
|------|----|---------|------|------|--------|-----------|
| EdgeX | deviceName | string | EVENTS| TagName | string | |
| EdgeX | origin | string | EVENTS| EventTime | timestamp | Converted to a timestamp |
| | | | EVENTS| Status | string | Can be defaulted in [RTDIP EdgeX to PCDM Component](../../sdk/code-reference/pipelines/transformers/spark/edgex_opcua_json_to_pcdm.md) otherwise Null |
| EdgeX | value | string | EVENTS | Value | dynamic | Converts Value into either a float number or string based on how it is received in the message |

### SSIP PI

[SSIP PI](https://bakerhughesc3.ai/oai-solution/shell-sensor-intelligence-platform/){target=_blank} connects to Osisoft PI Historians and sends the data to the Cloud.

The mapping below is performed by the RTDIP SSIP PI to PCDM Component and can be used in an [RTDIP Ingestion Pipeline.](../../sdk/pipelines/framework.md)

| From Data Model | From Field | From Type | To Data Model |To Field| To Type | Mapping Logic |
|------|----|---------|------|------|--------|-----------|
| SSIP PI | TagName | string | EVENTS| TagName | string | |
| SSIP PI | EventTime | string | EVENTS| EventTime | timestamp | |
| SSIP PI | Status | string | EVENTS| Status | string | |
| SSIP PI | Value | dynamic | EVENTS | Value | dynamic | |