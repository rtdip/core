# Functions

The RTDIP SDK enables users to perform complex queries, including aggregation on datasets within the Platform. Please find below the various types of queries available for specific dataset types. These SDK Functions are also supported by the [RTDIP API Docker Image.](https://hub.docker.com/r/rtdip/api)

## Time Series Events

### Raw

[Raw](../code-reference/query/functions/time_series/raw.md) facilitates performing raw extracts of time series data, typically filtered by a Tag Name or Device Name and an event time.

### Latest

[Latest](../code-reference/query/functions/time_series/latest.md) queries provides the latest event values. The RTDIP SDK requires the following parameters to retrieve the latest event values:
- TagNames - A list of tag names

### Resample

[Resample](../code-reference/query/functions/time_series/resample.md) enables changing the frequency of time series observations. This is achieved by providing the following parameters:

- Sample Rate - (<em>deprecated</em>)
- Sample Unit - (<em>deprecated</em>)
- Time Interval Rate - The time interval rate
- Time Interval Unit - The time interval unit (second, minute, day, hour)
- Aggregation Method - Aggregations including first, last, avg, min, max

!!! note "Note"
    </b>Sample Rate and Sample Unit parameters are deprecated and will be removed in v1.0.0. Please use Time Interval Rate and Time Interval Unit instead.<br />

### Interpolate

[Interpolate](../code-reference/query/functions/time_series/interpolate.md) - takes [resampling](#resample) one step further to estimate the values of unknown data points that fall between existing, known data points. In addition to the resampling parameters, interpolation also requires:

- Interpolation Method - Forward Fill, Backward Fill or Linear

### Interpolation at Time

[Interpolation at Time](../code-reference/query/functions/time_series/interpolation-at-time.md) - works out the linear interpolation at a specific time based on the points before and after. This is achieved by providing the following parameter:

- Timestamps - A list of timestamp or timestamps

### Time Weighted Averages

[Time Weighted Averages](../code-reference/query/functions/time_series/time-weighted-average.md) provide an unbiased average when working with irregularly sampled data. The RTDIP SDK requires the following parameters to perform time weighted average queries:

- Window Size Mins - (<em>deprecated</em>)
- Time Interval Rate - The time interval rate
- Time Interval Unit - The time interval unit (second, minute, day, hour)
- Window Length - Adds a longer window time for the start or end of specified date to cater for edge cases
- Step - Data points with step "enabled" or "disabled". The options for step are "true", "false" or "metadata" as string types. For "metadata", the query requires that the TagName has a step column configured correctly in the meta data table

!!! note "Note"
    </b>Window Size Mins is deprecated and will be removed in v1.0.0. Please use Time Interval Rate and Time Interval Unit instead.<br />

### Circular Averages

[Circular Averages](../code-reference/query/functions/time_series/circular-average.md) computes the circular average for samples in a range. The RTDIP SDK requires the following parameters to perform circular average queries:

- Time Interval Rate - The time interval rate
- Time Interval Unit - The time interval unit (second, minute, day, hour)
- Lower Bound - The lower boundary for the sample range
- Upper Bound - The upper boundary for the sample range

### Circular Standard Deviations

[Circular Standard Deviations](..//code-reference/query/functions/time_series/circular-standard-deviation.md) computes the circular standard deviations for samples assumed to be in the range. The RTDIP SDK requires the following parameters to perform circular average queries:

- Time Interval Rate - The time interval rate
- Time Interval Unit - The time interval unit (second, minute, day, hour)
- Lower Bound - The lower boundary for the sample range
- Upper Bound - The upper boundary for the sample range

## Time Series Metadata

### Metadata
[Metadata](../code-reference/query/functions/metadata.md) queries provide contextual information for time series measurements and include information such as names, descriptions and units of measure.


!!! note "Note"
    </b>RTDIP are continuously adding more to this list so check back regularly.<br />

## Query Examples
For examples of how to use the RTDIP functions, click the following links:

* [Raw](../examples/query/Raw.md)

* [Resample](../examples/query/Resample.md)

* [Interpolate](../examples/query/Interpolate.md)

* [Interpolation at Time](../examples/query/Interpolation-at-Time.md)

* [Time Weighted Averages](../examples/query/Time-Weighted-Average.md)

* [Circular Averages](../examples/query/Circular-Average.md)

* [Circular Standard Deviations](../examples/query/Circular-Standard-Deviation.md)

* [Metadata](../examples/query/Metadata.md)
