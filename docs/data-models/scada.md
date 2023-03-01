# Scada Data Model

## Overview

Overview of the Scada Data Model

## References

| Reference  | Description        |
|------------|--------------------|
|[IEC 61850](https://en.wikipedia.org/wiki/IEC_61850#:~:text=IEC%2061850%20is%20an%20international,architecture%20for%20electric%20power%20systems.)|Relevant description to IEC 61850|
|[IEC CIM](https://en.wikipedia.org/wiki/Common_Information_Model_(electricity))|Relevant description to IEC CIM|

## Schematic

``` mermaid
erDiagram
  METADATA ||--o{ EVENTS : contains
  METADATA {
    string TagName PK
    string description
    string UoM
    dict AdditionalProperties "Key Value pairs for system specific metadata"
  }
  EVENTS {
    date EventDate PK "Delta Partition Key"
    string TagName PK
    timestamp EventTime PK
    string Status
    list Value
    bool batch_task
  }
```

## Mappings

### OPC UA

| From | To Data Model | Details |
|------|----|---------|
| Time | EventTime| Maps as a timestamp|

