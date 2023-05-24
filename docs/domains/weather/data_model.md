# Weather Data Model

The weather data model describes available weather data schemas commonly used in weather services and available via RTDIP. 

### 15-day Hourly Forecast

This model returns weather forecasts starting with the current day. Forecast data is using TAF format, which is similar to METAR but contains extra forecast variables

|Type |Name |Description|  
|--|--------------------|------|
|String | class |Data identifier|
|Integer | clds |Hourly average cloud cover expressed as a percentage.|
|String | day_ind |This data field indicates whether it is daytime or nighttime based on the Local Apparent Time of the location.|
|Integer | dewpt |The temperature which air must be cooled at constant pressure to reach saturation. The Dew Point is also an indirect measure of the humidity of the air. The Dew Point will never exceed the Temperature. When the Dew Point and Temperature are equal, clouds or fog will typically form. The closer the values of Temperature and Dew Point, the higher the relative humidity.|
|String | dow |Day of week|
|Float | expire_time_gmt |Expiration time in UNIX seconds|
|Float | fcst_valid |Time forecast is valid in UNIX seconds|
|String | fcst_valid_local |Time forecast is valid in local apparent time|
|Integer | feels_like |Hourly feels like temperature. An apparent temperature. It represents what the air temperature “feels like” on exposed human skin due to the combined effect of the wind chill or heat index.|
|String | golf_category |The Golf Index Category expressed as a worded phrase the weather conditions for playing golf|
|Integer | golf_index |The Golf Index expresses on a scale of 0 to 10 the weather conditions for playing golf Not applicable at night. 0-2=Very Poor, 3=Poor, 4-5=Fair, 6-7=Good, 8-9=Very Good, 10=Excellent |
|Integer | gust |The maximum expected wind gust speed.|
|Integer | hi |Hourly maximum heat index. An apparent temperature. It represents what the air temperature “feels like” on exposed human skin due to the combined effect of warm temperatures and high humidity. When the temperature is 70°F or higher, the Feels Like value represents the computed Heat Index. For temperatures between 40°F and 70°F, the Feels Like value and Temperature are the same, regardless of wind speed and humidity, so use the Temperature value. |
|Integer | icon_code |This number is the key to the weather icon lookup. The data field shows the icon number that is matched to represent the observed weather conditions. Please refer to the Forecast Icon Code, Weather Phrases and Images document.|
|Integer | icon_extd |Code representing explicit full set sensible weather. Please refer to the Forecast Icon Code, Weather Phrases and Images document.|
|Float | mslp |Hourly mean sea level pressure|
|Integer | num |This data field is the sequential number that identifies each of the forecasted days in the API. They start on day 1, which is the forecast for the current day. Then the forecast for tomorrow uses number 2, then number 3 for the day after tomorrow, and so forth.|
|String | phrase_12char |Hourly sensible weather phrase|
|String | phrase_22char |Hourly sensible weather phrase|
|String | phrase_32char |Hourly sensible weather phrase|
|Integer | pop |Hourly maximum probability of precipitation|
|String | precip_type |The short text describing the expected type accumulation associated with the Probability of Precipitation (POP) display for the hour.|
|Float | qpf |The forecasted measurable precipitation (liquid or liquid equivalent) during the hour.|
|Integer | rh |The relative humidity of the air, which is defined as the ratio of the amount of water vapor in the air to the amount of vapor required to bring the air to saturation at a constant temperature. Relative humidity is always expressed as a percentage.|
|Integer | severity |A number denoting how impactful is the forecasted weather for this hour. Can be used to determine the graphical treatment of the weather display such as using red font on weather.com|
|Float | snow_qpf |The forecasted hourly snow accumulation during the hour.|
|String | subphrase_pt1 |Part 1 of 3-part hourly sensible weather phrase|
|String | subphrase_pt2 |Part 2 of 3-part hourly sensible weather phrase|
|String | subphrase_pt3 |Part 3 of 3-part hourly sensible weather phrase|
|Integer | temp |The temperature of the air, measured by a thermometer 1.5 meters (4.5 feet) above the ground that is shaded from the other elements. You will receive this data field in Fahrenheit degrees or Celsius degrees.|
|String | uv_desc |The UV Index Description which complements the UV Index value by providing an associated level of risk of skin damage due to exposure.|
|Integer | uv_index |Hourly maximum UV index|
|Decimal | uv_index_raw |The non-truncated UV Index which is the intensity of the solar radiation based on a number of factors.|
|Integer | uv_warning |TWC-created UV warning based on UV index of 11 or greater.|
|Decimal | vis |Prevailing hourly visibility|
|Integer | wc |Hourly minimum wind chill. An apparent temperature. It represents what the air temperature “feels like” on exposed human skin due to the combined effect of the cold temperatures and wind speed. When the temperature is 61°F or lower the Feels Like value represents the computed Wind Chill so display the Wind Chill value. For temperatures between 61°F and 75°F, the Feels Like value and Temperature are the same, regardless of wind speed and humidity, so display the Temperature value.|
|Integer | wdir |Hourly average wind direction in magnetic notation.|
|String | wdir_cardinal |Hourly average wind direction in cardinal notation.|
|Integer | wspd |The maximum forecasted hourly wind speed. The wind is treated as a vector; hence, winds must have direction and magnitude (speed). The wind information reported in the hourly current conditions corresponds to a 10-minute average called the sustained wind speed. Sudden or brief variations in the wind speed are known as “wind gusts” and are reported in a separate data field. Wind directions are always expressed as "from whence the wind blows" meaning that a North wind blows from North to South. If you face North in a North wind the wind is at your face. Face southward and the North wind is at your back. |
|String | wxman |Code combining Hourly sensible weather and temperature conditions|

## References

| Reference  | Description        |
|------------|--------------------|
|[METAR](https://en.wikipedia.org/wiki/METAR#:~:text=METAR%20is%20a%20format%20for,transmission%20of%20observational%20weather%20data.)| Description to METAR|
|[TAF](https://en.wikipedia.org/wiki/Terminal_aerodrome_forecast)|Description of TAF|