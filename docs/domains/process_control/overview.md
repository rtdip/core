# Process Control Domain Overview

For process control systems, RTDIP provides the ability to consume data from these sources, transform it and store the data in an open source format to enable:

- Data Science, ML and AI applications to consume the data
- Real time data in Digital Twins
- BI and Analytics 
- Reporting

## Process Control Systems

Process control systems monitor, control and safeguard production operations and generate vast amounts of data. Typical industry use cases include:

- Electricity Generation, Transmission and Distribution
- Chemicals, Gas, Oil Production and Distribution
- LNG Processing and Product Refining 

Process control systems record variables such as temperature, pressure, flow etc and automatically make adjustments to maintain preset specifications in a technical process.

This data can be made available to other systems over a number of protocols, such as [OPC UA.](https://opcfoundation.org/about/opc-technologies/opc-ua/) The protocols in turn make the data available to connectors that can send the data onwards to other systems and the cloud.

## Architecture

``` mermaid
graph LR
  A(Process Control) --> B(OPC UA Server);
  B --> C(Connectors);
  C --> D(Message Broker);
  D --> E(RTDIP);
  E --> F[(Destinations)];
```

### Connectors

A number of connectors are available from various suppliers. Some open source options include:

<center>[![Fledge](https://www.lfedge.org/wp-content/uploads/2019/09/fledge-horizontal-color.svg){width=40%}](https://www.lfedge.org/projects/fledge/) </center>

<center> [![Edge X Foundry](https://github.com/lf-edge/artwork/blob/master/edgexfoundry/horizontal/color/edgexfoundry-horizontal-color.png?raw=true){width=50%}](https://www.lfedge.org/projects/edgexfoundry/) </center>

### Message Brokers

Message Brokers support publishing of data from connectors and subscribing(pub/sub) to data from consumers. Popular options used with RTDIP are:

<center>[![Kafka](../process_control/images/kafka-logo-wide.png){width=40%}](https://kafka.apache.org/) </center>

<center>[![MQTT](../process_control/images/mqtt.png){width=40%}](https://mqtt.org/) </center>

<center>[![Azure IoT Hub](../process_control/images/iot_hub.png){width=40%}](https://azure.microsoft.com/en-us/products/iot-hub) </center>


## Real Time Data Ingestion Platform

For more information about the Real Time Data Platform and its components to connect to data sources and destinations, please refer to this [link.](../../sdk/overview.md)




