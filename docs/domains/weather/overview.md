
# Weather Services Overview

Many organizations need weather data for day-to-day operations. RTDIP provides the ability to consume data from examplar weather sources, transform it and store the data in an appropiate open source format to enable generic functions such as:

* Data Science, ML and AI applications to consume the data
* BI and Analytics
* Reporting

A primary aim for RTDIP in 2023 is to demonstrate how the platform can be utilized for domain specific services such as:

* Consumption Load Forecasting
* Energy Generation Forecasting
* Other behind the meter services and insights

Load forecasting is a technique used by power or energy-providing companies to predict the power/energy needed to meet the demand and supply equilibrium. Weather data is a primary driver of variance in load forecasting and energy generation forecasting in renewable energy sources. 

<!-- RTDIP utilizes a popular weather data format METAR, which originates in the aviation industry and is utilized by many others. -->

## Weather Data in Energy Domain

<!-- Weather data standards refer to a set of guidelines and protocols for collecting, processing, and disseminating weather data. These standards are important for ensuring that weather data is accurate, consistent, and can be easily shared and analyzed by different organizations and systems. -->

One of the most widely used weather data standards is the combined METAR (Meteorological Aerodrome Report) and ICAO (International Civil Aviation Organization) standard. This standard is used by meteorological agencies and aviation organizations around the world to report weather conditions at airports and other aviation facilities. This standard is broadly utilized beyond the aviation industry including the energy domain. 

The METAR ICAO standard includes a set of codes and abbreviations that describe weather conditions in a standardized format. These codes include information such as temperature, wind speed and direction, visibility, cloud cover, and precipitation. The standard also includes codes for reporting special weather phenomena, such as thunderstorms or volcanic ash.

<!-- In addition to the METAR ICAO standard, there are other weather data standards used for different purposes. For example, the WMO (World Meteorological Organization) has developed standards for exchanging weather data between national meteorological agencies, while the NOAA (National Oceanic and Atmospheric Administration) has developed standards for collecting and processing weather data in the United States. -->

Many actors in the energy domain utilize Historical, Forecast and near real-time METAR data as part of their services. Such data can be used to calculate average weather data by date and interval spanning multiple years, eg Historical Weather Data is often used to calculate an average or typical value for each weather variable eg. temperature, humidity over a given timeframe, which can be used for long range forecasting etc. 

 <!-- Forecast data is using TAF format, which is similar to METAR but contains extra forecast variables from IBM GRAF (IBM Global High-Resolution Atmospheric Forecasting System)  -->

<!-- COD -->
<!-- Diagram 1 describes a high level architecture design of Weather Data ingestion considering multiple weather data sources based on METAR ICAO type. All sources, Historical, Forecast and CoD(Current-On-Demand) share similar schemas. CoD is a near real-time actual weather reading.

<figure markdown>
  ![Weather Data Ingestion](images/image2.png "Weather Data Ingestion"){ width=100%}
  <figcaption>Diagram 1: High level Weather Data Ingestion</figcaption>
</figure>

# Innowatts

**Innowatts Competitive Edge **- is a SaaS offering providing data analytic services to the Energy domain, primarily the electrical domain. The platform offers services with respect to, but not limited to:

* Load forecasting
* Load scheduling
* Risk Mgmt
* Profitability
* Wholesale settlements -->

<!-- **Innowatts Weather Data** - Load forecasting i.e. predicting the consumption patterns of electrical users going forward is the primary service within Innowatts. Load forecasting is essential for the electrical domain to meet the demand and supply equilibrium**.**

The Innowatts weather service ingests METAR data, stores it in S3 and loads it into AWS Redshift where it is queryable via an API service. <em>See Diagram 3</em>.

<figure markdown>
  ![Innowatts Weather Service](images/image3.png "Innowatts Weather Service"){ width=100%}
  <figcaption>Diagram 3: Innowatts Weather Service</figcaption>
</figure>

This ingestion process is orchestrated by Airflow and DAG’s are scheduled according to source Data availability

* Historical Actuals: DAG runs twice a day
* 15-day Enhanced Forecast: DAG runs hourly
* Currents On Demand (CoD): DAG runs every 15min

The data is available for hundreds to thousands of METAR stations across the globe. -->


<!-- ## IW Weather Data Types

**CoD:** CoD is an external system that, at request time, assimilates a variety of meteorological inputs to derive a current condition value precise to the requested location on the Earth's surface. The meteorological inputs include physical surface observations, radar, satellite, lightning and short-term forecast models. The CoD system spatially and temporally blends each input appropriately at request-time, producing a result that improves upon any individual input used on its own.  

The CoD data feed returns a similar set of data elements as traditional site-based observations. The API provides information on temperature, precipitation, wind, barometric pressure, visibility

**15-Day Enhanced Forecast:** The Hourly Forecast returns weather forecasts starting with the current day at a 4 km resolution and includes relevant temperature, wind and precipitation features. 

**Cleaned Historical Actuals:** Provides a variety of observed and derived historical meteorological parameters including temperature, dewpoint, air pressure, wind speed and direction, relative humidity, degree day variables, as well as a set of specialized variables including soil moisture, sea level pressure, wind gust, cloud cover and others. Variables are available by latitude/longitude or specific location code. -->

# Weather Data Pipeline Examplar

An examplar pipeline is defined and provided within RTDIP. The overall approach and weather data in general is agnostic but the exemplar utilizes a specific external source. The overall ETL flow of the pipeline is outlined below:

1. Extraction: The specific source/connector aquires data from a specific external endpoint and persists the raw data into Deltalake 
2. Transformation:  An RTDIP transformer translates this raw data into a weather specific Delta schema.  
3. Loading: Essentially the function of loading is abstracted from the user and is handled by Deltalake.

Note, this is just an example implementation and different source and destination components could be utilized but if the underlying data model is altered or replaced then consuming downstream services would need to be adapted accordingly. 

The following is an indicative schema.

|Name |Description|
|--------------------|------|
| dateHrGmt |Greenwich Mean Time (GMT) date-time (also known as Universal Time)|
| dateHrLwt |Valid local date-time (Local wall time {includes daylight savings time})|
| surfaceTemperatureFahrenheit | Surface air (dry bulb) temperature at 2 meters|
| surfaceDewpointTemperatureFahrenheit |Atmospheric humidity metric (temperature at which dew will form)|
| surfaceWetBulbTemperatureFahrenheit | Atmospheric humidity metric (evaporative cooling potential of moist surface)|
| relativeHumidityPercent | Percent of water vapor in the air relative to its saturation point |
| apparentTemperatureFahrenheit | Air temperature that includes impact of wind and humidity |
| windChillTemperatureFahrenheit | Air temperature that includes impact of wind |
| heatIndexFahrenheit | Air temperature that includes the impact of humidity |
| precipitationPreviousHourInches | Liquid equivalent for types: warm rain, freezing rain, sleet, snow |
| snowfallInches | Total Snowfall |
| surfaceAirPressureMillibars | Atmospheric pressure at the Surface |
| mslPressureMillibars | Mean Sea Level Pressure |
| cloudCoveragePercent | Percentage of the sky covered by clouds |
| windSpeedMph | Unobstructed wind speed at 10 meters | 
| windDirectionDegrees | Upwind direction (e.g., wind from east = 90, from south = 180, etc.) at 10 meters |
| surfaceWindGustsMph | Unobstructed wind gusts at 10 meters |
| diffuseHorizontalRadiationWsqm | Diffuse (indirect) solar radiation flux on a plane parallel to the Earth's surface |
| directNormalIrradianceWsqm | Direct solar radiation flux on a surface 90 deg to the sun |
| downwardSolarRadiationWsqm | Total solar radiation flux on a plane parallel to the Earth's surface |
| surfaceTemperatureCelsius | Surface air (dry bulb) temperature at 2 meters |
| surfaceDewpointTemperatureCelsius | Atmospheric humidity metric (temperature at which dew will form) |
| surfaceWetBulbTemperatureCelsius | Atmospheric humidity metric (evaporative cooling potential of moist surface) |
| apparentTemperatureCelsius | Air temperature that includes impact of wind and humidity |
| windChillTemperatureCelsius | Air temperature that includes impact of wind |
| heatIndexCelsius | Air temperature that includes the impact of humidity |
| snowfallCentimeters | Total Snowfall | 
| precipitationPreviousHourCentimeters | Liquid equivalent for types: warm rain, freezing rain, sleet, snow |
| surfaceAirPressureKilopascals | Atmospheric pressure |
| mslPressureKilopascals | Mean Sea Level Pressure |
| surfaceWindGustsKph | Unobstructed wind gusts at 10 meters |
| windSpeedKph | Unobstructed wind speed at 10 meters |
| referenceEvapotranspiration | Reference Evapotranspiration (inches/hour) |
| dateHrGmt | Greenwich Mean Time (GMT) date-time (also known as Universal Time) |


# Real Time Data Ingestion Platform

For more information about the Real Time Data Platform and its components to connect to data sources and destinations, please refer to this [link](https://www.rtdip.io/sdk/overview/).

