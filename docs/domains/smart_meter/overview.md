# Smart Meter / Energy Domain
## Overview
Meter data is central to accelerating the electrification and decarbonisation of the energy grid. RTDIP provides the ability to consume meter data from exemplar sources, transform it and store it in an appropriate open-source format to enable domain-specific energy services, such as:

* Energy Load Forecasting
* Energy Generation Forecasting
* Other behind-the-meter services and insights

At a high level, the electricity system (US example) works as follows:

* **Generators**, of various types (coal, oil, natural gas, nuclear, wind turbines & PV, etc.) produce electricity 
* **Utilities**, distribute and transmit the electricity from the Generators through the grid to the  point of consumption i.e. buildings and homes
* **Suppliers**, wholesale purchase the electricity and sell it as retail contracts to Buyers
* **Buyers**, consume electricity, via buildings, homes, and electric vehicles, etc.
* **Consultants**, facilitate these transactions and/or offer data insights e.g. load forecasting to tailor purchasing, targeting reduced risk, profit, and competitive costs for Buyers

An **Independent System Operator (ISO)** sometimes called the Regional Transmission Organisation (RTO) is an organisation that is in charge of the entire process. They coordinate, control, and monitor the electric grid in a specific region, typically a multi-state area.

## Meter Data Pipelines
Load forecasting is a technique used by ISO's, and energy-providing companies to predict the power/energy needed to meet the demand and supply equilibrium of the energy grid. RTDIP defines and provides example pipelines for the two primary inputs to energy services like load forecasting, namely [weather](../../domains/weather/overview.md) and meter data.

Specifically, with respect to meter data RTDIP defines and provides two exemplar ISO's:

* the Midcontinent Independent System Operator, [MISO](https://www.misoenergy.org/about/)   
* the PJM Interconnection LLC Independent System Operator, [PJM](https://www.pjm.com/about-pjm)

## Architecture

The overall ETL flow of the pipeline is outlined below:

``` mermaid
graph LR
  A(External Meter Source e.g. MISO, PJM) --> B(RTDIP Source/Connector);
  B --> C(RTDIP Transformer);
  C --> D(RTDIP Source/Connector);
  D --> E[(RTDIP Destination)];
```

1. Source: The specific source/connector acquires data from a specific external endpoint (MISO or PJM) and persists the raw data into Deltalake 
2. Transformer:  An RTDIP transformer translates this raw data into a meter specific Delta schema.  
3. Destination: Essentially the function of loading is abstracted from the user and is handled by Deltalake. 

Indicative schema are available [here](data_model.md). 

# Real Time Data Ingestion Platform
For more information about the Real Time Data Platform and its components to connect to data sources and destinations, please refer to this [link](../../sdk/overview.md).