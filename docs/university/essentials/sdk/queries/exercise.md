Its time to start running some time series queries using the RTDIP SDK.

1. Using the python file you created in the previous exercise, import the necessary time series query classes from the RTDIP SDK.

2. Pass the connector you created in the previous exercise to the time series query class.

3. Run a `Raw` query to retrieve some data from your time series data source.

4. Now run a query to `Resample` this data to a 15 minute interval average.

5. Convert the resample query to an `Interpolation` query that executes the `linear` interpolation method.

6. Finally, try running a `Time Weighted Average` query on the data, with `Step` set to False.

## Additional Task

7. The data returned from these queries is in the form of a pandas DataFrame. Use the `matplotlib` or `plotly` library to plot the data returned from the `Time Weighted Average` query.

<br></br>
[← Previous](./weather.md){ .curved-button }
[Next →](../../powerbi/overview.md){ .curved-button }

## Course Progress
-   [X] Introduction
-   [X] SDK
    *   [X] Authentication
    *   [X] Connectors
    *   [X] Queries
-   [ ] Power BI    
-   [ ] APIs
-   [ ] Excel Connector

