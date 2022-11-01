# Real Time Data Ingestion Platform (RTDIP)

| Branch | Workflow Status | Code Coverage | Vulnerabilities | Bugs |
|--------|-----------------|---------------|----------|------|
| main | [![Main](https://github.com/rtdip/core/actions/workflows/main.yml/badge.svg?branch=develop)](https://github.com/rtdip/core/actions/workflows/main.yml) | [![Coverage](https://sonarcloud.io/api/project_badges/measure?project=rtdip_core&metric=coverage&branch=main)](https://sonarcloud.io/summary/new_code?id=rtdip_core) | [![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=rtdip_core&metric=vulnerabilities&branch=main)](https://sonarcloud.io/summary/new_code?id=rtdip_core) | [![Bugs](https://sonarcloud.io/api/project_badges/measure?project=rtdip_core&metric=bugs&branch=main)](https://sonarcloud.io/summary/new_code?id=rtdip_core) |
| develop | [![Develop](https://github.com/rtdip/core/actions/workflows/develop.yml/badge.svg)](https://github.com/rtdip/core/actions/workflows/develop.yml) | [![Coverage](https://sonarcloud.io/api/project_badges/measure?project=rtdip_core&metric=coverage&branch=develop)](https://sonarcloud.io/summary/new_code?id=rtdip_core) | [![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=rtdip_core&metric=vulnerabilities&branch=develop)](https://sonarcloud.io/summary/new_code?id=rtdip_core) | [![Bugs](https://sonarcloud.io/api/project_badges/measure?project=rtdip_core&metric=bugs&branch=develop)](https://sonarcloud.io/summary/new_code?id=rtdip_core) |
| feature | [![.github/workflows/pr.yml](https://github.com/rtdip/core/actions/workflows/pr.yml/badge.svg)](https://github.com/rtdip/core/actions/workflows/pr.yml) |

This repository contains Real Time Data Ingestion Platform SDK functions and documentation. This README will be a developer guide to understand the repository.

## What is RTDIP SDK?

​​**Real Time Data Ingestion Platform (RTDIP) SDK** is a software development kit built to easily access some of RTDIP's transformation functions.

The RTDIP SDK will give the end user the power to use some of the convenience methods for frequency conversions and resampling of Pi data all through a self-service platform. RTDIP is offering a flexible product with the ability to authenticate and connect to Databricks SQL Warehouses given the end users preferences. RTDIP have taken the initiative to cut out the middle man and instead wrap these commonly requested methods in a simple python module so that you can instead focus on the data. 

See [RTDIP Documentation](https://www.rtdip.io/) for more information on how to use the SDK.

# Repository Guidelines
The structure of this repository is shown below in the tree diagram.

    ├── .devcontainer
    ├── .github
    │   ├── workflows
    ├── docs
    │   ├── api
    │   ├── assets
    │   ├── blog
    │   ├── getting-started
    │   ├── images
    │   ├── integration
    │   ├── releases
    │   ├── roadmap
    │   ├── sdk
    │   ├── index.md
    ├── src
    │   ├── api
    │   │   ├── assets
    │   │   ├── auth
    │   │   ├── FastAPIApp
    │   │   ├── v1
    │   ├── apps
    │   │   ├── docs
    │   ├── sdk
    │   │   ├── python
    │   │   │    ├── rtdip-sdk
    │   │   │    │   ├── authentication
    │   │   │    │   ├── functions
    │   │   │    │   ├── odbc
    ├── tests
    │   ├── api
    │   │   ├── auth
    │   │   ├── v1
    │   ├── apps
    │   │   ├── docs
    │   ├── sdk
    │   │   ├── python
    │   │   │    ├── rtdip-sdk
    │   │   │    │   ├── authentication
    │   │   │    │   ├── functions
    │   │   │    │   ├── odbc
    ├── CODE_OF_CONDUCT.md  
    ├── CODEOWNERS.md 
    ├── CONTRIBUTING.md
    ├── GOVERNANCE.md
    ├── LICENSE.md
    ├── RELEASE.md
    ├── SUPPORT.md
    ├── PYPI-README.md
    ├── environment.yml
    ├── mkdocs.yml
    ├── setup.py   
    └── .gitignore

## Folder Structure

| Folder Name        | Description                                                          |
|--------------------|----------------------------------------------------------------------|
|`.github/workflows` | yml files for Github Action workflows                                | 
|`.devcontainer`     | Setup for VS Code Remote Containers and Github Spaces                |
|`docs`              | Documentation in markdown, organised by subject name at folder level |
|`src`               | Main projects and all souce code, organised by language and sdk name |
|`tests`             | Test projects and unit tests, organised by language and sdk name     |

## File Structure

| File Name        | Description                                                                             |
|------------------|-----------------------------------------------------------------------------------------|
|`mkdocs.yml`      | yml file to host all documentation on mkdocs                                            |
|`setup.py`        | Set up requirements for python package deployment                                       |
|`environment.yml` | yml file to create an environment with all the dependencies for developers              |
|`CODE_OF_CONDUCT` | code of conduct                                                                         |
|`CODEOWNERS`      | codeowners                                                                              |
|`CONTRIBUTING.yml`| contributing                                                                            |
|`GOVERNANCE.yml`  | governance                                                                              |
|`LICENSE.yml`     | license                                                                                 |
|`RELEASE.yml`     | releases                                                                                |
|`SUPPORT.yml`     | support                                                                                 |
|`PYPI-README.yml` | PyPi read me documentation                                                              |
|`README.yml`      | RTDIP read me documentation                                                             |
|`.gitignore`      | Informs Git which files to ignore when committing your project to the GitHub repository |

# Developer Guide

## Getting Started 

1) To get started with developing for this project, clone the repository. 
```
    git clone https://github.com/rtdip/core.git
```
2) Open the respository in VS Code, Visual Studio or your preferered code editor.

3) Create a new environment using the following command:
```
    conda env create -f environment.yml
```

> **_NOTE:_**  You will need to have conda, python and pip installed to use the command above.

4) Activate your newly set up environment using the following command:
```
    conda activate rtdip-sdk
```
You are now ready to start developing your own functions. Please remember to follow RTDIP's development lifecycle to maintain clarity and efficiency for a fully robust self serving platform. 
    
## RTDIP Development Lifecycle

1) Develop

2) Write unit tests

3) Document

4) Publish

> **_NOTE:_**  Ensure you have read the [Release Guidelines](RELEASE.md) before publishing your code.

# Contribution
This project welcomes contributions and suggestions. If you have a suggestion that would make this better you can simply open an issue with a relevant title. Don't forget to give the project a star! Thanks again!

For more details on contributing to this repository, see the [Contibuting Guide](https://github.com/rtdip/core/blob/develop/CONTRIBUTING.md). Please read this projects [Code of Conduct](https://github.com/rtdip/core/blob/develop/CODE_OF_CONDUCT.md) before contributing.

## Documentation
This repository has been configured with support documentation for Real Time Data Ingestion Platform (RTDIP) to make it easier to get started. RTDIP's documentation is created using mkdocs and is hosted on Github Pages.

* Documentation can be found on [RTDIP Documentation](https://www.rtdip.io/)

# Licensing

Distributed under the Apache License Version 2.0. See [LICENSE.md](https://github.com/rtdip/core/blob/develop/LICENSE.md) for more information.

# Need help?
* For reference documentation, pre-requisites, getting started, tutorials and examples visit [RTDIP Documentation](https://www.rtdip.io/). 
* File an issue via [Github Issues](https://github.com/rtdip/core/issues).
* Check previous questions and answers or ask new ones on our slack channel [**#real-time-data-ingestion-platform**](https://join.slack.com/t/lfenergy/shared_invite/zt-1ilkyecnq-8TDP6pzZXnmx1o0Lc~kMcA)

### Community
* Chat with other community members by joining the **#real-time-data-ingestion-platform** Slack channel. [Click here to join our slack community](https://join.slack.com/t/lfenergy/shared_invite/zt-1ilkyecnq-8TDP6pzZXnmx1o0Lc~kMcA)