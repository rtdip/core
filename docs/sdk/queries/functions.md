# Functions
<!-- --8<-- [start:intro] -->
The RTDIP SDK enables users to perform complex queries, including aggregation on datasets within the Platform. Please find below the various types of queries available for specific dataset types. These SDK Functions are also supported by the [RTDIP API Docker Image.](https://hub.docker.com/r/rtdip/api)
<!-- --8<-- [end:intro] -->

## Time Series Events

### Raw
<!-- --8<-- [start:raw] -->
[Raw](https://www.rtdip.io/sdk/code-reference/query/functions/time_series/raw/) facilitates performing raw extracts of time series data, typically filtered by a Tag Name or Device Name and an event time.
<!-- --8<-- [end:raw] -->
### Latest
<!-- --8<-- [start:latest] -->
[Latest](https://www.rtdip.io/sdk/code-reference/query/functions/time_series/latest/) queries provides the latest event values. The RTDIP SDK requires the following parameters to retrieve the latest event values:
- TagNames - A list of tag names
<!-- --8<-- [end:latest] -->
### Resample
<!-- --8<-- [start:resample] -->
[Resample](https://www.rtdip.io/sdk/code-reference/query/functions/time_series/resample/) enables changing the frequency of time series observations. This is achieved by providing the following parameters:

- Time Interval Rate - The time interval rate
- Time Interval Unit - The time interval unit (second, minute, day, hour)
- Aggregation Method - Aggregations including first, last, avg, min, max
<!-- --8<-- [end:resample] -->

### Plot
<!-- --8<-- [start:plot] -->
[Plot](https://www.rtdip.io/sdk/code-reference/query/functions/time_series/plot/) enables changing the frequency of time series observations and performing Average, Min, Max, First, Last and StdDev aggregations. This is achieved by providing the following parameters:

- Time Interval Rate - The time interval rate
- Time Interval Unit - The time interval unit (second, minute, day, hour)
<!-- --8<-- [end:plot] -->

### Interpolate
<!-- --8<-- [start:interpolate] -->
[Interpolate](https://www.rtdip.io/sdk/code-reference/query/functions/time_series/plot/) - takes [resampling](#resample) one step further to estimate the values of unknown data points that fall between existing, known data points. In addition to the resampling parameters, interpolation also requires:

- Interpolation Method - Forward Fill, Backward Fill or Linear
<!-- --8<-- [end:interpolate] -->
### Interpolation at Time
<!-- --8<-- [start:interpolateattime] -->
[Interpolation at Time](https://www.rtdip.io/sdk/code-reference/query/functions/time_series/interpolate-at-time/) - works out the linear interpolation at a specific time based on the points before and after. This is achieved by providing the following parameter:

- Timestamps - A list of timestamp or timestamps
<!-- --8<-- [end:interpolateattime] -->
### Time Weighted Averages
<!-- --8<-- [start:timeweightedaverage] -->
[Time Weighted Averages](https://www.rtdip.io/sdk/code-reference/query/functions/time_series/time-weighted-average/) provide an unbiased average when working with irregularly sampled data. The RTDIP SDK requires the following parameters to perform time weighted average queries:

- Time Interval Rate - The time interval rate
- Time Interval Unit - The time interval unit (second, minute, day, hour)
- Window Length - Adds a longer window time for the start or end of specified date to cater for edge cases
- Step - Data points with step "enabled" or "disabled". The options for step are "true", "false" or "metadata" as string types. For "metadata", the query requires that the TagName has a step column configured correctly in the meta data table
<!-- --8<-- [end:timeweightedaverage] -->

### Circular Averages
<!-- --8<-- [start:circularaverages] -->
[Circular Averages](https://www.rtdip.io/sdk/code-reference/query/functions/time_series/circular-average/) computes the circular average for samples in a range. The RTDIP SDK requires the following parameters to perform circular average queries:

- Time Interval Rate - The time interval rate
- Time Interval Unit - The time interval unit (second, minute, day, hour)
- Lower Bound - The lower boundary for the sample range
- Upper Bound - The upper boundary for the sample range
<!-- --8<-- [end:circularaverages] -->
### Circular Standard Deviations
<!-- --8<-- [start:circularstandarddeviation] -->
[Circular Standard Deviations](https://www.rtdip.io/sdk/code-reference/query/functions/time_series/circular-standard-deviation/) computes the circular standard deviations for samples assumed to be in the range. The RTDIP SDK requires the following parameters to perform circular average queries:

- Time Interval Rate - The time interval rate
- Time Interval Unit - The time interval unit (second, minute, day, hour)
- Lower Bound - The lower boundary for the sample range
- Upper Bound - The upper boundary for the sample range
<!-- --8<-- [end:circularstandarddeviation] -->
### Summary
<!-- --8<-- [start:summary] -->
[Summary](https://www.rtdip.io/sdk/code-reference/query/functions/time_series/summary/) computes a summary of statistics (Avg, Min, Max, Count, StDev, Sum, Variance).
<!-- --8<-- [end:summary] -->

## Time Series Metadata

### Metadata
<!-- --8<-- [start:metadata] -->
[Metadata](https://www.rtdip.io/sdk/code-reference/query/functions/metadata/) queries provide contextual information for time series measurements and include information such as names, descriptions and units of measure.
<!-- --8<-- [end:metadata] -->

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
