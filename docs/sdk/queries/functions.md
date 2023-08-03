# Functions

The RTDIP SDK enables users to perform complex queries, including aggregation on datasets within the Platform. Please find below the various types of queries available for specific dataset types. These SDK Functions are also supported by the [RTDIP API Docker Image.](https://hub.docker.com/r/rtdip/api)

## Time Series Events

### Raw

[Raw](../code-reference/query/raw.md) facilitates performing raw extracts of time series data, typically filtered by a Tag Name or Device Name and an event time.

### Resample

[Resample](../code-reference/query/resample.md) enables changing the frequency of time series observations. This is achieved by providing the following parameters:

- Sample Rate - (<em>deprecated</em>)
- Sample Unit - (<em>deprecated</em>)
- Time Interval Rate - The time interval rate
- Time Interval Unit - The time interval unit (second, minute, day, hour)
- Aggregation Method - Aggregations including first, last, avg, min, max

!!! note "Note"
    </b>Sample Rate and Sample Unit parameters are deprecated and will be removed in v1.0.0. Please use Time Interval Rate and Time Interval Unit instead.<br />

### Interpolate

[Interpolate](../code-reference/query/interpolate.md) - takes [resampling](#resample) one step further to estimate the values of unknown data points that fall between existing, known data points. In addition to the resampling parameters, interpolation also requires:

- Interpolation Method - Forward Fill or Backward Fill

### Interpolation at Time

[Interpolation at Time](../code-reference/query/interpolation_at_time.md) - works out the linear interpolation at a specific time based on the points before and after. This is achieved by providing the following parameter:

- Timestamps - A list of timestamp or timestamps

### Time Weighted Averages

[Time Weighted Averages](../code-reference/query/time-weighted-average.md) provide an unbiased average when working with irregularly sampled data. The RTDIP SDK requires the following parameters to perform time weighted average queries:

- Window Size Mins - (<em>deprecated</em>)
- Time Interval Rate - The time interval rate
- Time Interval Unit - The time interval unit (second, minute, day, hour)
- Window Length - Adds a longer window time for the start or end of specified date to cater for edge cases
- Step - Data points with step "enabled" or "disabled". The options for step are "true", "false" or "metadata" as string types. For "metadata", the query requires that the TagName has a step column configured correctly in the meta data table

!!! note "Note"
    </b>Window Size Mins is deprecated and will be removed in v1.0.0. Please use Time Interval Rate and Time Interval Unit instead.<br />

## Time Series Metadata

### Metadata
[Metadata](../code-reference/query/metadata.md) queries provide contextual information for time series measurements and include information such as names, descriptions and units of measure.

!!! note "Note"
    </b>RTDIP are continuously adding more to this list so check back regularly.<br />

## Query Examples

1\. To use any of the RTDIP functions, use the commands below.

```python
from rtdip_sdk.queries import resample
from rtdip_sdk.queries import interpolate
from rtdip_sdk.queries import interpolation_at_time
from rtdip_sdk.queries import raw
from rtdip_sdk.queries import time_weighted_average
from rtdip_sdk.queries import metadata
```

2\. From functions you can use any of the following methods.

#### Resample
    resample.get(connection, parameters_dict)

#### Interpolate
    interpolate.get(connection, parameters_dict)

#### Interpolation at Time
    interpolation_at_time.get(connection, parameters_dict)

#### Raw
    raw.get(connection, parameters_dict)

#### Time Weighted Average
    time_weighted_average.get(connection, parameter_dict)

#### Metadata
    metadata.get(connection, parameter_dict)
