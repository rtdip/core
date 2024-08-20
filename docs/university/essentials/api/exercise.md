# Exercise

In this exercise, you will learn how to run APIs on Swagger and Postman. 

1. Go to your RTDIP Swagger page and click the green Authorize button on the right.

2. Call a `Raw Get` query to retrieve some data from your time series data source.

3. Now call a query to `Resample Get` this data to a 15 minute interval average.

4. Convert the resample query to an `Interpolation Get` query that executes the `linear` interpolation method.

5. Finally, try calling a `Time Weighted Average Get` query on the data, with `Step` set to False.

## Additional Task

7. Similarly, on Postman, run `Raw Get` to retrieve some data from your time series data source. You will need to pass a bearer token in the Authorization section. 

8. Repeat exercise 2-5 using Postman.

<br></br>
[← Previous](./postman.md){ .curved-button }
[Next →](../excel-connector/overview.md){ .curved-button }

## Course Progress
-   [X] Introduction
-   [X] SDK
-   [X] Power BI
-   [X] APIs
    *   [X] Overview
    *   [X] Authentication
    *   [X] Swagger
    *   [X] Postman
    *   [X] Exercise
-   [ ] Excel Connector