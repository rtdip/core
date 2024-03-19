---
date: 2024-03-14
authors:
  - GBARAS
---

# Energy Forecasting: Utilising the Power of Tomorrow’s Data

<center>
![EnergyForecastingImage](../images/energy-forecasting.png){width=75%} 
</center>

Energy forecasting plays a pivotal role in our modern world, where energy consumption, production and pricing are critical factors. 

Energy forecasting involves predicting the demand load and price of various energy sources, including both fossil fuels and renewable energy resources like wind and solar.

With an accurate energy usage forecast, a business can efficiently allocate and manage resources, this is crucial to maintain a stable energy supply to the consumer; energy forecasting is fundamental as we transition to renewable energy sources which do not produce consistent energy. Energy companies, grid operators and industrial consumers rely on forecasts to optimize their operations. Over- or undercontracting can lead to significant financial losses, so precise forecasts are essential.

<!-- more -->

Energy load prices and forecasts greatly influence the energy sector and the decisions made across multiple departments in energy companies.  For example, energy forecasts are vital for planning and investing in new capacity, they guide decisions on new assets,  transmission lines and distribution networks. Another example is risk mitigation, unstable electricity prices can be handled with accurate forecasting of the market, companies can develop bidding strategies, production schedules and consumption patterns to minimize risk and maximize profits.

A rough estimate of savings from a 1% reduction in the mean absolute percentage error (MAPE) for a utility with a 1 GW peak load includes: 

-	$500,000 per year from long-term load forecasting
-	$300,000 per year from short-term load forecasting
-	$600,000 per year from short-term load and price forecasting

Energy Forecasting allows for significant cost avoidance due to better price forecasts and risk management.

## Energy Forecasting with RTDIP

RTDIP can be a powerful tool for businesses looking to forecast energy usage. RTDIP supports load forecasting, a critical technique used by ISOs (Independent System Operators) and energy providers. Load forecasting allows a business to predict the power or energy needed to maintain the balance between energy demand and supply on the grid. Two primary inputs for load forecasting are weather data and meter data, RTDIP has developed pipeline components for these types of data.

RTDIP provides example pipelines for weather forecast data ingestion. Accurate weather data helps predict energy production in renewable assets based on factors like temperature, humidity and wind patterns.

RTDIP defines example pipelines for meter data from ISOs like MISO and PJM. Meter data includes consumption patterns, load profiles and real-time measurements. The transformers in RTDIP can translate raw meter data into suitable data models for efficient storage and analysis.

The data models in RTDIP are IEC CIM (Common Information Model) for time series and metering data. This ensures compatibility with systems requiring data aligning with the IEC CIM standard.

## Building Pipelines for Energy Forecasting

RTDIP allows you to develop and deploy cloud agnostic pipelines with popular orchestration engines. There are a number of RTDIP components focused on weather and metering data, these are all listed below.

### Sources

[MISO Daily Load ISO](https://www.rtdip.io/sdk/code-reference/pipelines/sources/spark/iso/miso_daily_load_iso/)

The MISO Daily Load ISO Source is used to read daily load data from MISO API. It supports both Actual and Forecast data. The [MISO API](https://docs.misoenergy.org/marketreports/) can be found here.

[MISO Historical Load ISO](https://www.rtdip.io/sdk/code-reference/pipelines/sources/spark/iso/miso_historical_load_iso/)

The MISO Historical Load ISO Source is used to read historical load data from MISO API. The [MISO API](https://docs.misoenergy.org/marketreports/) can be found here.

[PJM Daily Load ISO](https://www.rtdip.io/sdk/code-reference/pipelines/sources/spark/iso/pjm_daily_load_iso/)	

The PJM Daily Load ISO Source is used to read daily load data from PJM API. It supports both Actual and Forecast data. Actual will return 1 day, Forecast will return 7 days. The [PJM API](https://api.pjm.com/api/v1/) can be found here. 

[PJM Historical Load ISO](https://www.rtdip.io/sdk/code-reference/pipelines/sources/spark/iso/pjm_historical_load_iso/)
The PJM Historical Load ISO Source is used to read historical load data from PJM API. The [PJM API](https://api.pjm.com/api/v1/) can be found here. 

[CAISO Daily Load ISO](https://www.rtdip.io/sdk/code-reference/pipelines/sources/spark/iso/caiso_daily_load_iso/)

The CAISO Daily Load ISO Source is used to read daily load data from CAISO API. It supports multiple types of data. Check the load_types attribute. The [CAISO API](http://oasis.caiso.com/oasisapi) can be found here. 

[CAISO Historical Load ISO](https://www.rtdip.io/sdk/code-reference/pipelines/sources/spark/iso/caiso_historical_load_iso/)

The CAISO Historical Load ISO Source is used to read load data for an interval of dates between start_date and end_date inclusive from CAISO API. It supports multiple types of data. Check the load_types attribute. The [CAISO API](http://oasis.caiso.com/oasisapi) can be found here. 

[ERCOT Daily Load ISO](https://www.rtdip.io/sdk/code-reference/pipelines/sources/spark/iso/ercot_daily_load_iso/)

The ERCOT Daily Load ISO Source is used to read daily load data from ERCOT using WebScrapping. It supports actual and forecast data. The [ERCOT API](https://mis.ercot.com) can be found here. 

[Weather Forecast API V1](https://www.rtdip.io/sdk/code-reference/pipelines/sources/spark/the_weather_company/weather_forecast_api_v1/)		

The Weather Forecast API V1 Source is used to read 15 days forecast from the Weather API. The [Weather API](https://api.weather.com/v1/geocode/32.3667/-95.4/forecast/hourly/360hour.json) can be found here.

[Weather Forecast API V1 Multi](https://www.rtdip.io/sdk/code-reference/pipelines/sources/spark/the_weather_company/weather_forecast_api_v1_multi/)	

The Weather Forecast API V1 Multi Source is used to read 15 days forecast from the Weather API. It allows to pull weather data for multiple stations and returns all of them in a single DataFrame. The [Weather API](https://api.weather.com/v1/geocode/32.3667/-95.4/forecast/hourly/360hour.json) for one station can be found here.

[ECMWF MARS Weather Forecast](https://www.rtdip.io/sdk/code-reference/pipelines/sources/spark/ecmwf/weather_forecast/)

The class SparkECMWFWeatherForecastSource allows users to access the ECMWF MARS Server via an API, to access historical forecast data and download as a .nc file. More information about th [ECMWF MARS Server](https://confluence.ecmwf.int/display/UDOC/MARS+user+documentation) can be found here.


### Transformers

[MISO To Meters Data Model](https://www.rtdip.io/sdk/code-reference/pipelines/transformers/spark/iso/miso_to_mdm/)

Converts MISO Raw data into Meters Data Model.

[PJM To Meters Data Model](https://www.rtdip.io/sdk/code-reference/pipelines/transformers/spark/iso/pjm_to_mdm/)

Converts PJM Raw data into Meters Data Model.

[CAISO To Meters Data Model](https://www.rtdip.io/sdk/code-reference/pipelines/transformers/spark/iso/caiso_to_mdm/)

Converts CAISO Raw data into Meters Data Model.

[ERCOT To Meters Data Model](https://www.rtdip.io/sdk/code-reference/pipelines/transformers/spark/iso/ercot_to_mdm/)

Converts ERCOT Raw data into Meters Data Model.

[Raw Forecast to Weather Data Mode](https://www.rtdip.io/sdk/code-reference/pipelines/transformers/spark/the_weather_company/raw_forecast_to_weather_data_model/)

Converts a raw forecast from the The Weather Company into weather data model.

[ECMWF NC Forecast Extract Point To Weather Data Model](https://www.rtdip.io/sdk/code-reference/pipelines/transformers/spark/ecmwf/nc_extractpoint_to_weather_data_model/)

The class ECMWFExtractPointToWeatherDataModel allows users to extract the forecast at a singular latitude and longitude point from a .nc file which has been downloaded and stored locally from a request to the ECMWF MARS server. The forecast is extracted and transformed to the Weather data model.

[ECMWF NC Forecast Extract Grid To Weather Data Model](https://www.rtdip.io/sdk/code-reference/pipelines/transformers/spark/ecmwf/nc_extractgrid_to_weather_data_model/)

The class ECMWFExtractGridToWeatherDataModel allows users to extract the forecast for grid area from a .nc file which has been downloaded and stored locally from a request to the ECMWF MARS server. The forecast is extracted and transformed to the Weather data model.


## Contribute 

RTDIP empowers energy professionals to share solutions, RTDIP welcomes contributions and recognises the importance of sharing code. There are multiple sources for weather and metering data crucial to forecasting energy needs, if you have anymore you’d like to add to RTDIP please raise a feature request and contribute.
 
