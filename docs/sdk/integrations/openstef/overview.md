# RTDIP Integration with OpenSTEF

OpenSTEF offers a comprehensive software solution for predicting electricity grid load for the next 48 hours. RTDIP has developed a [Database connector](../../../code-reference/integrations/openstef/database.md) that enables the execution of OpenSTEF pipelines on data that has been ingested using RTDIP.

## Prerequisites

Ensure that you have followed the installation instructions as specified in the [Getting Started](../../../../getting-started/installation.md) section and follow the steps which highlight the installation requirements for using Integrations. In particular:

* [RTDIP SDK Installation](../../../../getting-started/installation.md#installing-the-rtdip-sdk)

!!! note "RTDIP SDK installation"
    Ensure you have installed the RTDIP SDK, as a minimum, as follows:
    ```
    pip install "rtdip-sdk[integrations]"
    ```

    For all installation options please see the RTDIP SDK installation [instructions.](../../../../getting-started/installation.md#installing-the-rtdip-sdk)

## Overview
OpenSTEF offers automated machine learning pipelines to generate short term load forecasts. It leverages both historical grid data and external predictors like weather and market prices, aiding in anticipating load congestion and maximizing asset utilization. 

Tasks within OpenSTEF can be used to perform training, forecasting, and evaluation pipelines while handling the fetching of configuration and feature data from a database, task exceptions, and writing data back to the database. Machine learning is a key component of these pipelines, facilitating task execution and managing the resulting models with MLflow.

RTDIP has simplified the usage of OpenSTEF tasks and pipelines with data ingested through RTDIP by introducing a [Database connector](../../../code-reference/integrations/openstef/database.md). This connector serves as an interface for feature and configuration data stored in Databricks that adhere to our data models.

## Conclusion
Find out more about OpenSTEF [here](https://openstef.github.io/openstef/index.html). For examples of how to use the [Database connector](../../../code-reference/integrations/openstef/database.md), click on the following links:

* [Train Models](../../../examples/integrations/openstef/train-model.md)
* [Create Forecasts](../../../examples/integrations/openstef/create-forecast.md)