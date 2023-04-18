
# Weather Data 

![lf-energy-rtdip](images/image1.png "lf-energy-rtdip"){width=100%}

Organizations need data for day-to-day operations and to support advanced Data Science, Statistical and Machine Learning capabilities such as Optimization, Surveillance, Forecasting, and Predictive Analytics. Real time data & batch data forms a major part of the total data utilized in these activities.

Data enables organizations to detect and respond to changes in their systems thus improving the efficiency of their operations. Additionally, batch and mini-batch data form the crux of many organisational offerings. This range of data needs to be available in scalable and secure data platforms.

**Real Time Data Ingestion Platform (RTDIP)** is a **PaaS** (Platform as a Service) which coupled with some custom components provides Data Ingestion, Data Transformation, and Data Sharing as a service. RTDIP can interface with several data sources to ingest many different data types which include time series, alarms, video, photos and audio being provided from sources such as Historians, OPC Servers and Rest APIs, as well as data being sent from hardware such as IoT Sensors, Robots and Drones

# Introduction

A primary aim for RTDIP in 2023 is to demonstrate how the platform can be utilized for domain specific services such as load forecasting in the Energy Domain. Load forecasting is a technique used by power or energy-providing companies to predict the power/energy needed to meet the demand and supply equilibrium.

Weather data is a primary driver of variance in load forecasting. RTDIP aims to ingest, transform, store and provide access to such generic data which can then be utilised in a domain specific context.

This document will summarize the most popular Weather Data formats, its usage in Business Services and expand to describe how weather data can be supported within RTDIP. 

# Weather Data

## Weather Data Standards

Weather data standards refer to a set of guidelines and protocols for collecting, processing, and disseminating weather data. These standards are important for ensuring that weather data is accurate, consistent, and can be easily shared and analyzed by different organizations and systems.

One of the most widely used weather data standards is the METAR (Meteorological Aerodrome Report) and ICAO (International Civil Aviation Organization) standard. This standard is used by meteorological agencies and aviation organizations around the world to report weather conditions at airports and other aviation facilities. 

The METAR ICAO standard includes a set of codes and abbreviations that describe weather conditions in a standardized format. These codes include information such as temperature, wind speed and direction, visibility, cloud cover, and precipitation. The standard also includes codes for reporting special weather phenomena, such as thunderstorms or volcanic ash.

The METAR ICAO standard is designed to be easily understood and used by pilots, air traffic controllers, and meteorologists. It provides a common language for reporting weather conditions, allowing aviation organizations to make informed decisions about flight operations and safety.

In addition to the METAR ICAO standard, there are other weather data standards used for different purposes. For example, the WMO (World Meteorological Organization) has developed standards for exchanging weather data between national meteorological agencies, while the NOAA (National Oceanic and Atmospheric Administration) has developed standards for collecting and processing weather data in the United States.

Innowatts is using Historical METAR data, containing a set of selected standard weather variables (temperature, cloud cover, humidity etc).  Aggregated METAR weather information is used in IW Models. This data comes from permanent weather observation stations on a typical report cadence of 15min to 1hr.

## Innowatts Weather Data Formats

Historical Data is used to calculate Normals which is an average of 21 years and TMY (Typical Meteorological Year).

Forecast data is using TAF format, which is similar to METAR but contains extra forecast variables from IBM GRAF (IBM Global High-Resolution Atmospheric Forecasting System) 

Diagram 1 describes a high level architecture design of Weather Data ingestion considering multiple weather data sources based on METAR ICAO type. All sources (Historical, Forecast and CoD) share similar schemas. More details will be provided later in this document. 

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
* Wholesale settlements

**Innowatts Weather Data** - Load forecasting i.e. predicting the consumption patterns of electrical users going forward is the primary service within Innowatts. Load forecasting is essential for the electrical domain to meet the demand and supply equilibrium**.**

The Innowatts weather service ingests METAR data, stores it in S3 and loads it into AWS Redshift where it is queryable via an API service. <em>See Diagram 3</em>.

<figure markdown>
  ![Innowatts Weather Service](images/image3.png "Innowatts Weather Service"){ width=100%}
  <figcaption>Diagram 3: Innowatts Weather Service</figcaption>
</figure>

This ingestion process is orchestrated by Airflow and DAG’s are scheduled according to source Data availability

* Historical Actuals: DAG runs twice a day
* 15-day Enhanced Forecast: DAG runs hourly
* Currents On Demand (CoD): DAG runs every 15min

The data is available for hundreds to thousands of METAR stations across the globe.


## IW Weather Data Types

**CoD:** CoD is an external system that, at request time, assimilates a variety of meteorological inputs to derive a current condition value precise to the requested location on the Earth's surface. The meteorological inputs include physical surface observations, radar, satellite, lightning and short-term forecast models. The CoD system spatially and temporally blends each input appropriately at request-time, producing a result that improves upon any individual input used on its own.  

The CoD data feed returns a similar set of data elements as traditional site-based observations. The API provides information on temperature, precipitation, wind, barometric pressure, visibility

**15-Day Enhanced Forecast:** The Hourly Forecast returns weather forecasts starting with the current day at a 4 km resolution and includes relevant temperature, wind and precipitation features. 

**Cleaned Historical Actuals:** Provides a variety of observed and derived historical meteorological parameters including temperature, dewpoint, air pressure, wind speed and direction, relative humidity, degree day variables, as well as a set of specialized variables including soil moisture, sea level pressure, wind gust, cloud cover and others. Variables are available by latitude/longitude or specific location code.

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




Below code snippet demonstrates transformation of direct API output within data service.

```python
        for record in weather_hours:
            date_time = datetime.strptime(
                record["dateHrLwt"], "%m/%d/%Y %H:%M:%S")

            r = OrderedDict(
                [
                    ("weather_id", weatherstation),
                    ("weather_day", date_time.strftime("%Y-%m-%d")),
                    ("weather_hour", date_time.hour + 1),
                    ("weather_type", "A"),
                    ("processed_date", processed_date),
                    (
                        "temperature",
                        float(record["surfaceTemperatureFahrenheit"])
                        if float(record["surfaceTemperatureFahrenheit"])
                        else None,
                    ),
                    (
                        "dew_point",
                        float(record["surfaceDewpointTemperatureFahrenheit"])
                        if float(record["surfaceDewpointTemperatureFahrenheit"])
                        else None,
                    ),
                    (
                        "humidity",
                        float(record["relativeHumidityPercent"])
                        if float(record["relativeHumidityPercent"])
                        else None,
                    ),
                    (
                        "heat_index",
                        float(record["heatIndexFahrenheit"])
                        if float(record["heatIndexFahrenheit"])
                        else None,
                    ),
```
<figcaption>IW Historical Actuals (METAR) Mapping Example</figcaption>

# RTDIP - Weather Data Ingestion Process

Its proposed that weather data (being domain agnostic) be ingested within RTDIP following the standard ETL (Extract Transform Load) process outlined in the documentation. It extracts data from various sources, transforms it into a format that is suitable for utilization, and then loads data into a warehouse of other storage systems. Finally, data should be accessible by end services by enabling querying methods or API’s. 

In the context of weather data, the ETL process using a transformer may involve the following steps:

1. Extraction: The first step in the ETL process is to extract the weather data from various sources, such as weather stations or online weather services. The data is typically in a raw format, such as CSV or JSON.
2. Transformation: The next step is to transform the extracted data into a format that is suitable for analysis. The transformer applies a set of rules or transformations to the data, such as cleaning and filtering the data, converting units of measure, aggregating the data, and creating new features or variables.
3. Loading: Once the data has been transformed, the next step is to load it into a data warehouse or other storage system for analysis. The transformed data may be loaded into a structured database, such as SQL Server or Oracle, or into a non-structured data store, such as Hadoop or Spark.


## Transformer and Connector

**A connector** is a software component that facilitates the transfer of data between different systems. In the context of a data lake, a connector may be used to move data from a source system to the data lake, where it can be stored and made available for analysis by various services.

To enable the data stored in a data lake to be queried by services, the connector may use a queryable data storage format, such as Parquet,ORC or CSV. These formats are designed to enable efficient querying of large datasets and can be easily integrated with various analytics tools and services.

The connector may also need to perform additional processing steps, such as data transformation or schema mapping, to ensure that the data is compatible with the data lake and the queryable format being used..  For example, the connector may need to convert the data into a common format, such as JSON or Avro, or apply data cleansing or filtering rules to ensure the data is accurate and consistent.  In context of RTDIP we will consider a Transformer component to handler data transformation

Once the data has been transferred to the data lake and stored in a queryable format, it can be easily accessed and analyzed by various services, such as data visualization tools or machine learning algorithms. The data lake may also provide additional features and services, such as data cataloging or data governance, to help users manage and analyze the data more effectively.

Example of connector [Code 1] takes in API key as input, which is used to make API call to the OpenWeatherMap API. It then creates Pandas DataFrame with extracted data and returns it. 
<!-- 
<figure markdown>
  ![Weather Data Connector](images/image5.png "Weather Data Connector"){ width=100%}
  <figcaption>Code 1: Example of a weather data connector</figcaption>
</figure> -->

```python
import requests
import pandas as pd

class WeatherDataConnector:
    def __init__(self, api_key):
        self.api_key = api_key
        
    def get_data(self, city, country):
        url = f'https://api.openweathermap.org/data/2.5/weather?q={city},{country}&appid={self.api_key}'
        response = requests.get(url)
        data = response.json()

        # Extract relevant data from the API response
        temperature = data['main']['temp']
        pressure = data['main']['pressure']
        humidity = data['main']['humidity']
        wind_speed = data['wind']['speed']
        wind_direction = data['wind']['deg']
        weather_condition = data['weather'][0]['main']

        # Create a pandas DataFrame with the extracted data
        weather_data = pd.DataFrame({
            'city': [city],
            'country': [country],
            'temperature': [temperature],
            'pressure': [pressure],
            'humidity': [humidity],
            'wind_speed': [wind_speed],
            'wind_direction': [wind_direction],
            'weather_condition': [weather_condition]
        })

        return weather_data
```
<figcaption>Code 1: Example of a weather data connector</figcaption>

**A transformer** is a component in the ETL process that is responsible for the data transformation step. The transformer takes the extracted data, applies a set of rules or transformations to it, and then outputs the transformed data in a format that is suitable for loading into the target system.

The transformer plays a critical role in the ETL process, as it is responsible for the data transformation step, which can be complex and time-consuming. The transformer may be implemented using various technologies, such as SQL scripts, Python scripts, or ETL tools such as Apache NiFi, Apache Kafka or Apache Storm.

Example of Transformer [Code 2] takes in a pandas DataFrame containing weather data with columns date and temperature and transforms it in the following ways:

* Converts the date column to datetime format
* Splits the date column into separate year, month, and day columns
* Converts the temperature from Fahrenheit to Celsius (example of mapping)
* Drops the date and temperature columns
* Fills in missing values in the temperature_celsius column with the mean temperature
* Groups the data by year, month, and day and calculates the mean temperature for each group

<!-- <figure markdown>
  ![Simple Transformation](images/image6.png "Simple Transformation"){ width=100%}
  <figcaption>Code 2: Example of a simple transformation</figcaption>
</figure> -->

```python
import pandas as pd
import numpy as np

class WeatherDataTransformer:
    def __init__(self):
        pass
    
    def transform(self, data):
        # Convert date column to datetime format
        data['date'] = pd.to_datetime(data['date'], format='%Y-%m-%d')

        # Split date into year, month, and day columns
        data['year'] = data['date'].dt.year
        data['month'] = data['date'].dt.month
        data['day'] = data['date'].dt.day

        # Convert temperature from Fahrenheit to Celsius
        data['temperature_celsius'] = (data['temperature'] - 32) * 5/9

        # Drop date and temperature columns
        data = data.drop(columns=['date', 'temperature'])

        # Fill missing values with mean temperature
        mean_temperature = np.mean(data['temperature_celsius'])
        data['temperature_celsius'] = data['temperature_celsius'].fillna(mean_temperature)

        # Group by year, month, and day and calculate mean temperature
        data = data.groupby(['year', 'month', 'day'], as_index=False).mean()

        return data
```
<figcaption>Code 2: Example of a simple transformation</figcaption>

## Accessibility & Egress

Once weather data has been loaded into a data lake, it can be made accessible to various systems and services through several methods:

1. Querying with SQL: If the weather data has been loaded into a relational database or a data lake with a SQL-based query engine, users can query the data using SQL statements. SQL is a widely used language for querying and analyzing data, and many data visualization tools and reporting platforms support SQL.
2. Using RESTful APIs: Many data lakes provide RESTful APIs that allow users to access and retrieve data programmatically. Users can interact with the API by making HTTP requests and receiving responses in a variety of formats, such as JSON, CSV, or XML.
3. Integrating with Business Intelligence (BI) Tools: Business intelligence tools, such as Tableau, Power BI, or QlikView, can be integrated with a data lake to access and visualize weather data. These tools provide a variety of data visualization and exploration capabilities, allowing users to create dashboards and reports that provide insights into the weather data.
