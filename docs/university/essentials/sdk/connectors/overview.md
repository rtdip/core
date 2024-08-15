# RTDIP Connectors

Integration and connectivity to RTDIP is facilitated through the use of connectors. Users require connectivity from various tools and applications to RTDIP and the connectors provided with the RTDIP SDK enable this. As an overview of the connectors, the following are available:

- Databricks SQL Connector: This is the default connector used in the SDK and is the simplest to use as there are no additional installation requirements. Additionally, this connector provides adequate performance for most use cases.
- ODBC Connector: In certain scenarios, users may want to leverage the Spark SIMBA ODBC driver. This requires the user to install and setup the driver in their environment prior to use, after which it can leverage Turbodbc or Pyodbc for connectivity to RTDIP.
- Spark Connector: This connector supports workloads that are running in a spark environment such as Databricks or where Spark Connect is required.

<br></br>
[← Previous](../authentication/databricks.md){ .curved-button }
[Next →](./databricks-sql-connector.md){ .curved-button }

## Course Progress
-   [X] Overview
-   [X] Architecture
-   [ ] SDK
    *   [X] Getting Started
    *   [X] Authentication
    *   [ ] Connectors
        +   [X] Overview    
        +   [ ] Databricks SQL
        +   [ ] ODBC
        +   [ ] Spark
        +   [ ] Exercise
    *   [ ] Queries
-   [ ] Power BI
-   [ ] APIs
-   [ ] Excel Connector