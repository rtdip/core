
# Weather Services 
## Overview
Many organizations need weather data for day-to-day operations. RTDIP provides the ability to consume data from examplar weather sources, transform it and store the data in an appropriate open source format to enable generic functions such as:

* Data Science, ML and AI applications to consume the data
* BI and Analytics
* Reporting

A primary aim for RTDIP in 2023 is to demonstrate how the platform can be utilised for domain specific services such as:

* Consumption Load Forecasting
* Energy Generation Forecasting
* Other behind the meter services and insights

Weather data is a primary driver,  together with [meter](../../domains/smart_meter/overview.md) data, of variance in load & generation forecasting in the energy domain. 

## Weather Data in the Energy Domain

One of the most widely used weather data standards is the combined METAR (Meteorological Aerodrome Report) and ICAO (International Civil Aviation Organization) standard. This standard is used by meteorological agencies and aviation organizations around the world to report weather conditions at airports and other aviation facilities. This standard is broadly utilised beyond the aviation industry including the energy domain. 

The METAR ICAO standard includes a set of codes and abbreviations that describe weather conditions in a standardized format. These codes include information such as temperature, wind speed and direction, visibility, cloud cover, and precipitation. The standard also includes codes for reporting special weather phenomena, such as thunderstorms or volcanic ash.

Many actors in the energy domain utilise Historical, Forecast and near real-time METAR data as part of their services. Such data can be used to calculate average weather data by date and interval spanning multiple years, eg Historical Weather Data is often used to calculate an average or typical value for each weather variable eg. temperature, humidity over a given timeframe, which can be used for long range forecasting etc. 

## Architecture

An exemplar pipeline is defined and provided within RTDIP. The overall approach and weather data in general is agnostic but the exemplar utilises a specific external source. The overall ETL flow of the pipeline is outlined below:


``` mermaid
graph LR
  A(External Weather Source) --> B(RTDIP Source/Connector);
  B --> C(RTDIP Transformer);
  C --> D(RTDIP Source/Connector);
  D --> E[(RTDIP Destination)];
```


1. Source: The specific source/connector aquires data from a specific external endpoint and persists the raw data into Deltalake 
2. Tranformer:  An RTDIP transformer translates this raw data into a weather specific Delta schema.  
3. Destination: Essentially the function of loading is abstracted from the user and is handled by Deltalake. 

An indicative schema is available [here](data_model.md). 


# Real Time Data Ingestion Platform

For more information about the Real Time Data Platform and its components to connect to data sources and destinations, please refer to this [link](../../sdk/overview.md).

