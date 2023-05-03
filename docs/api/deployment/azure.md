# Deploy RTDIP APIs to Azure

The RTDIP repository contains the code to deploy the RTDIP REST APIs to your own Azure Cloud environment. The APIs are built as part of the rtdip repository CI/CD pipelines and the image is deployed to Docker Hub repo `rtdip/api`. Below contains information on how to build and deploy the containers from source or to setup your function app to use the deployed container image provided by RTDIP.

## Deploying the RTDIP APIs

### Deployment from Build

To deploy the RTDIP APIs directly from the repository, follow the steps below:

1. Build the docker image using the following command:
    ```bash
    docker build --tag <container_registry_url>/rtdip-api:v0.1.0 -f src/api/Dockerfile .
    ```
1. Login to your container registry
    ```bash
    docker login <container_registry_url>
    ```
1. Push the docker image to your container registry
    ```bash
    docker push <container_registry_url>/rtdip-api:v0.1.0
    ```
1. Configure your Function App to use the docker image
    ```bash
    az functionapp config container set --name <function_app_name> --resource-group <resource_group_name> --docker-custom-image-name <container_registry_url>/rtdip-api:v0.1.0
    ```

### Deployment from Docker Hub

To deploy the RTDIP APIs from Docker Hub, follow the steps below:

1. Configure your Function App to use the docker image
    ```bash
    az functionapp config container set --name <function_app_name> --resource-group <resource_group_name> --docker-custom-image-name rtdip/api:azure-<version>
    ```

### Environment Variables

#### Azure Active Directory
1. Once Authentication has been configured on the Azure Function App correctly, it is required to set the following Environment Variable with the Tenant ID of the relevant Active Directory:
    - TENANT_ID

#### Databricks
1. The following Environment Variables are required and the values can be retrieved from your Databricks SQL Warehouse or Databricks Cluster:
    - DATABRICKS_SQL_SERVER_HOSTNAME
    - DATABRICKS_SQL_HTTP_PATH

#### ODBC Driver
1. To allow the APIs to leverage Turbodbc for connectivity and possible performance improvements, it is possible to set the following environment variable:
    - RTDIP_ODBC_CONNECTION=turbodbc
