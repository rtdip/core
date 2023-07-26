# Getting Started with Azure Functions for RTDIP REST APIs

## Specific Dev Information for RTDIP REST APIs

[Fast API](https://fastapi.tiangolo.com/) is the Web Framework used for developing the APIs. It is recommended to read the documentation to understand the functionality it provides to quickly create REST APIs. Additionally, Azure Functions support Fast API with [documentation and examples](https://docs.microsoft.com/en-us/samples/azure-samples/fastapi-on-azure-functions/azure-functions-python-create-fastapi-app/) for your reference.

Due to the package installation requirements for RTDIP SDK, it is required to deploy the Azure Functions using a docker container. It is fairly simple to test your development in docker, by executing the following steps

### Docker development

Ensure that you are in the root folder of the repository when you run the below commands. This ensures the correct folders can be copied into the container from the repo.

```bash
docker build --tag rtdip-api:v0.1.0 -f src/api/Dockerfile .
docker run -p 8080:80 -it rtdip-api:v0.1.0
```

For Macbooks with Apple Silicon chips, use the following:

```bash
docker build --platform linux/amd64 --tag rtdip-api:v0.1.0 -f src/api/Dockerfile .
docker run --platform linux/amd64 -p 8080:80 -it --env-file .env rtdip-api:v0.1.0
```

REST APIs are then available at `http://localhost:8080/api/v1/{route}`

### Debugging

It is also possible to debug using the standard debugger in VS Code. The `.vscode` folder contains the relevant settings to automatically start debugging the APIs. **NOTE** that the endpoints for debugger sessions are `http://localhost:7071/api/v1/{route}`

Ensure that you setup the **local.settings.json** file with the relevant parameters to execute the code on your machine. The below Databricks SQL settings are available in Databricks workspaces.

|Environment Variable| Value |
|---------|-------|
|DATABRICKS_SQL_SERVER_HOSTNAME|adb-xxxxx.x.azuredatabricks.net|
|DATABRICKS_SQL_HTTP_PATH|/sql/1.0/warehouses/xxx|

### Swagger and Redoc

Fast API provides endpoints for Swagger and Redoc pages. Ensure that you review these pages after any updates to confirm they are working as expected.

## General Dev Information for developing APIs using Azure Functions

Below is general information regarding Azure Functions which is the framework for creating REST APIs for RTDIP. This enables serverless capabilities that allows for easy scaling the APIs according to demand.

### Project Structure
The main project folder (<project_root>) can contain the following files:

* **local.settings.json** - Used to store app settings and connection strings when running locally. This file doesn't get published to Azure. To learn more, see [local.settings.file](https://aka.ms/azure-functions/python/local-settings).
* **requirements.txt** - Contains the list of Python packages the system installs when publishing to Azure.
* **host.json** - Contains global configuration options that affect all functions in a function app. This file does get published to Azure. Not all options are supported when running locally. To learn more, see [host.json](https://aka.ms/azure-functions/python/host.json).
* **.vscode/** - (Optional) Contains store VSCode configuration. To learn more, see [VSCode setting](https://aka.ms/azure-functions/python/vscode-getting-started).
* **.venv/** - (Optional) Contains a Python virtual environment used by local development.
* **Dockerfile** - (Optional) Used when publishing your project in a [custom container](https://aka.ms/azure-functions/python/custom-container).
* **tests/** - (Optional) Contains the test cases of your function app. For more information, see [Unit Testing](https://aka.ms/azure-functions/python/unit-testing).
* **.funcignore** - (Optional) Declares files that shouldn't get published to Azure. Usually, this file contains .vscode/ to ignore your editor setting, .venv/ to ignore local Python virtual environment, tests/ to ignore test cases, and local.settings.json to prevent local app settings being published.

Each function has its own code file and binding configuration file ([**function.json**](https://aka.ms/azure-functions/python/function.json)).

### Developing your first Python function using VS Code

If you have not already, please checkout our [quickstart](https://aka.ms/azure-functions/python/quickstart) to get you started with Azure Functions developments in Python. 

### Publishing your function app to Azure 

For more information on deployment options for Azure Functions, please visit this [guide](https://docs.microsoft.com/en-us/azure/azure-functions/create-first-function-vs-code-python#publish-the-project-to-azure).

### Next Steps

* To learn more about developing Azure Functions, please visit [Azure Functions Developer Guide](https://aka.ms/azure-functions/python/developer-guide).

* To learn more specific guidance on developing Azure Functions with Python, please visit [Azure Functions Developer Python Guide](https://aka.ms/azure-functions/python/python-developer-guide).