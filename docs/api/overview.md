<center> ![rest](images/rest-api-logo.png){width=50%} </center>

<!-- --8<-- [start:restapi] -->

# RTDIP REST APIs

RTDIP provides REST API endpoints for querying data in the platform. The APIs are a wrapper to the python [RTDIP SDK](../sdk/overview.md) and provide similar functionality for users and applications that are unable to leverage the python RTDIP SDK. It is recommended to read the [RTDIP SDK documentation](../sdk/overview.md) and in particular the [Functions](../sdk/code-reference/query/functions/time_series/resample.md) section for more information about the options and logic behind each API. 

RTDIP API's are designed with the intention of running small to medium queries, rather than large queries to reduce network latency, increase performance and maintainability.  

<!-- --8<-- [end:restapi] -->