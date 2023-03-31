![RTDIP](images/rtdip-horizontal-color.png){width=100%}

Organizations need data for day-to-day operations and to support advanced Data Science, Statistical and Machine Learning capabilities such as Optimization, Surveillance, Forecasting, and Predictive Analytics. **Real Time Data** forms a major part of the total data utilized in these activities.

Real time & near real time data enables organizations to detect and respond to changes in their systems thus improving the efficiency of their operations. Additionally, batch and mini-batch data form the crux of many organisational offerings. This range of data needs to be available in scalable and secure data platforms.

**Real Time Data Ingestion Platform (RTDIP)** is a **PaaS** (Platform as a Service) which coupled with some custom components provides Data Ingestion, Data Transformation, and Data Sharing as a service. RTDIP can interface with several data sources to ingest many different data types which include time series, alarms, video, photos and audio being provided from sources such as Historians, OPC Servers and Rest APIs, as well as data being sent from hardware such as IoT Sensors, Robots and Drones

**Innowatts Competitive Edge **- is a SaaS offering providing data analytic services to the Energy domain, primarily electrical domain. The platform offers services with respect to but not limited to:



* Load forecasting
* Load scheduling
* Risk Mgmt \


**Innowatts Weather Data** - Load forecasting i.e. predicting the consumption patterns of electrical users going forward is the primary service within Innowatts. Load forecasting is essential for the electrical domain to meet the demand and supply equilibrium**.**

A primary aim for RTDIP in 2023 is to demonstrate how the platform can be ustilsied for domain specific services such as load forecasting. 

Therefore, the platform must be capable of ingesting, transforming, storing and providing access to the data inputs that drive such services including weather data which is the primary driver of variance in load forecasting. 

This document will summarize usage of Weather Data in IW & Shell Services & expand to describe how weather data is supported within RTDIP. 

Weather Data Standards 

Weather data is a broad data category. It can be divided into subcategories based on temporal and geographical coverage. There are multiple available forms of reporting weather information, such as METAR [1], CLIMAT, BUFR, TAF[2] etc.

Raw METAR is the most common format in the world for the transmission of observational weather data. It is highly standardized through the International Civil Aviation Organization (ICAO), which allows it to be understood throughout most of the world. [3][4]

Innowatts is using Historical METAR data, containing a set of selected standard weather variables (temperature, cloud cover, humidity etc).  Aggregated METAR weather information is used in IW Models. This data comes from permanent weather observation stations on a typical report cadence of 15min to 1hr.

Historical Data is used to calculate Normals which is an average of 21 years and TMY (Typical Meteorological Year).

Forecast data is using TAF format, which is similar to METAR but contains extra forecast variables from IBM GRAF[5] (IBM Global High-Resolution Atmospheric Forecasting System) 



[ToDo add image 2]


Innowatts Weather Data Architecture



[ToDo add image 3]


High level Weather Data Ingestion



[ToDo add image 4]


Innowatts Weather Service

Innowatts Historical Standard Weather Variables



[ToDo add image 5]


Weather Data Types in Innowatts


<table>
  <tr>
   <td><strong>Name</strong>
   </td>
   <td><strong>Description</strong>
   </td>
  </tr>
  <tr>
   <td><strong>dateHrGmt</strong>
   </td>
   <td>Greenwich Mean Time (GMT) date-time (also known as Universal Time)
   </td>
  </tr>
  <tr>
   <td><strong>dateHrLwt</strong>
   </td>
   <td>Valid local date-time (Local wall time {includes daylight savings time})
   </td>
  </tr>
  <tr>
   <td><strong>surfaceTemperatureFahrenheit</strong>
   </td>
   <td>Surface air (dry bulb) temperature at 2 meters
   </td>
  </tr>
  <tr>
   <td><strong>surfaceDewpointTemperatureFahrenheit</strong>
   </td>
   <td>Atmospheric humidity metric (temperature at which dew will form)
   </td>
  </tr>
  <tr>
   <td><strong>surfaceWetBulbTemperatureFahrenheit</strong>
   </td>
   <td>Atmospheric humidity metric (evaporative cooling potential of moist surface)
   </td>
  </tr>
  <tr>
   <td><strong>relativeHumidityPercent</strong>
   </td>
   <td>Percent of water vapor in the air relative to its saturation point
   </td>
  </tr>
  <tr>
   <td><strong>apparentTemperatureFahrenheit</strong>
   </td>
   <td>Air temperature that includes impact of wind and humidity
   </td>
  </tr>
  <tr>
   <td><strong>windChillTemperatureFahrenheit</strong>
   </td>
   <td>Air temperature that includes impact of wind
   </td>
  </tr>
  <tr>
   <td><strong>heatIndexFahrenheit</strong>
   </td>
   <td>Air temperature that includes the impact of humidity
   </td>
  </tr>
  <tr>
   <td><strong>precipitationPreviousHourInches</strong>
   </td>
   <td>Liquid equivalent for types: warm rain, freezing rain, sleet, snow
   </td>
  </tr>
  <tr>
   <td><strong>snowfallInches</strong>
   </td>
   <td>Total Snowfall
   </td>
  </tr>
  <tr>
   <td><strong>surfaceAirPressureMillibars</strong>
   </td>
   <td>Atmospheric pressure at the Surface
   </td>
  </tr>
  <tr>
   <td><strong>mslPressureMillibars</strong>
   </td>
   <td>Mean Sea Level Pressure
   </td>
  </tr>
  <tr>
   <td><strong>cloudCoveragePercent</strong>
   </td>
   <td>Percentage of the sky covered by clouds
   </td>
  </tr>
  <tr>
   <td><strong>windSpeedMph</strong>
   </td>
   <td>Unobstructed wind speed at 10 meters
   </td>
  </tr>
  <tr>
   <td><strong>windDirectionDegrees</strong>
   </td>
   <td>Upwind direction (e.g., wind from east = 90, from south = 180, etc.) at 10 meters
   </td>
  </tr>
  <tr>
   <td><strong>surfaceWindGustsMph</strong>
   </td>
   <td>Unobstructed wind gusts at 10 meters
   </td>
  </tr>
  <tr>
   <td><strong>diffuseHorizontalRadiationWsqm</strong>
   </td>
   <td>Diffuse (indirect) solar radiation flux on a plane parallel to the Earth's surface
   </td>
  </tr>
  <tr>
   <td><strong>directNormalIrradianceWsqm</strong>
   </td>
   <td>Direct solar radiation flux on a surface 90 deg to the sun
   </td>
  </tr>
  <tr>
   <td><strong>downwardSolarRadiationWsqm</strong>
   </td>
   <td>Total solar radiation flux on a plane parallel to the Earth's surface
   </td>
  </tr>
  <tr>
   <td><strong>surfaceTemperatureCelsius</strong>
   </td>
   <td>Surface air (dry bulb) temperature at 2 meters
   </td>
  </tr>
  <tr>
   <td><strong>surfaceDewpointTemperatureCelsius</strong>
   </td>
   <td>Atmospheric humidity metric (temperature at which dew will form)
   </td>
  </tr>
  <tr>
   <td><strong>surfaceWetBulbTemperatureCelsius</strong>
   </td>
   <td>Atmospheric humidity metric (evaporative cooling potential of moist surface)
   </td>
  </tr>
  <tr>
   <td><strong>apparentTemperatureCelsius</strong>
   </td>
   <td>Air temperature that includes impact of wind and humidity
   </td>
  </tr>
  <tr>
   <td><strong>windChillTemperatureCelsius</strong>
   </td>
   <td>Air temperature that includes impact of wind
   </td>
  </tr>
  <tr>
   <td><strong>heatIndexCelsius</strong>
   </td>
   <td>Air temperature that includes the impact of humidity
   </td>
  </tr>
  <tr>
   <td><strong>snowfallCentimeters</strong>
   </td>
   <td>Total Snowfall
   </td>
  </tr>
  <tr>
   <td><strong>precipitationPreviousHourCentimeters</strong>
   </td>
   <td>Liquid equivalent for types: warm rain, freezing rain, sleet, snow
   </td>
  </tr>
  <tr>
   <td><strong>surfaceAirPressureKilopascals</strong>
   </td>
   <td>Atmospheric pressure
   </td>
  </tr>
  <tr>
   <td><strong>mslPressureKilopascals</strong>
   </td>
   <td>Mean Sea Level Pressure
   </td>
  </tr>
  <tr>
   <td><strong>surfaceWindGustsKph</strong>
   </td>
   <td>Unobstructed wind gusts at 10 meters
   </td>
  </tr>
  <tr>
   <td><strong>windSpeedKph</strong>
   </td>
   <td>Unobstructed wind speed at 10 meters
   </td>
  </tr>
  <tr>
   <td><strong>referenceEvapotranspiration</strong>
   </td>
   <td>Reference Evapotranspiration (inches/hour)
   </td>
  </tr>
  <tr>
   <td><strong>dateHrGmt</strong>
   </td>
   <td>Greenwich Mean Time (GMT) date-time (also known as Universal Time)
   </td>
  </tr>
</table>


Enhanced 15-day Forecast:

    {

        "v3-wx-forecast-hourly-2day": {

            "cloudCover": [30,45],

            "dayOfWeek": ["Saturday","Saturday"]

            "dayOrNight": ["D","D"],

            “temperatureDewPoint”: [60,100],

            "expirationTimeUtc": [1474132031,1474132031],

            "iconCode": [30,30],

            "iconCodeExtend": [3000,3000],

            "precipChance": [0,0],

            "precipType": ["rain","rain" ],

            "pressureMeanSeaLevel": [30.1,30.07 ],

            "qpf": [0,0 ],

            "qpfSnow": [0,0 ],

            "relativeHumidity": [56,50 ],

            "temperature": [84,86 ],

            "temperatureFeelsLike": [87,89 ],

            "temperatureHeatIndex": [87,89 ],

            "temperatureWindChill": [84,86 ],

            "uvDescription": ["Very High","Very High" ],

            "uvIndex": [8,8 ],

            "validTimeLocal": ["2016-09-17T13:00:00-0400","2016-09-17T14:00:00-0400" ],

            "validTimeUtc": [1474131600,1474135200 ],

            "visibility": [10,10 ],

            "windDirection": [129,137 ],

            "windDirectionCardinal": ["SE","SE" ],

            "windGust": [null,null ],

            "windSpeed": [8,7 ],

            "wxPhraseLong": ["Partly Cloudy","Partly Cloudy" ],

            "wxPhraseShort": ["P Cloudy","P Cloudy" ],

            "wxSeverity": [1,1 ]

        }

    }

What are the questions/gaps? 

1: Will Weather Data be treated as a separate module within RTDIP ingestion? 

2: Multiple Sources/Formats - Mapping between sources -> pre-defined RTDIP schema? 

3: Transformer for each weather reporting format: METAR, Ocean, CLIMAT etc

[1] METAR

[https://www.aviationweather.gov/dataserver/output?datatype=metar](https://www.aviationweather.gov/dataserver/output?datatype=metar)

[2] TAF

 [https://www.aviationweather.gov/dataserver/output?datatype=taf](https://www.aviationweather.gov/dataserver/output?datatype=taf)

[3] METAR ICAO

[https://en.wikipedia.org/wiki/METAR](https://en.wikipedia.org/wiki/METAR)

[4] METAR ICAO

[https://en.wikipedia.org/wiki/METAR](https://en.wikipedia.org/wiki/METAR)

[5] IBM GRAF

https://www.ibm.com/weather/industries/cross-industry/graf