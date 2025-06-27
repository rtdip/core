# Real Time Data Ingestion Platform (RTDIP)

<p align="center"><img src=https://raw.githubusercontent.com/rtdip/core/develop/docs/getting-started/images/rtdip-horizontal-color.png alt="rtdip" width=50% height=50%/></p>

<div align="center">

[![PyPI version](https://img.shields.io/pypi/v/rtdip-sdk.svg?logo=pypi&logoColor=FFE873)](https://pypi.org/project/rtdip-sdk/)
[![Supported Python versions](https://img.shields.io/pypi/pyversions/rtdip-sdk.svg?logo=python&logoColor=FFE873)](https://pypi.org/project/rtdip-sdk/)
[![PyPI downloads](https://img.shields.io/pypi/dm/rtdip-sdk.svg)](https://pypistats.org/packages/rtdip-sdk)
![PyPI Downloads](https://static.pepy.tech/badge/rtdip-sdk)
[![OpenSSF Best Practices](https://bestpractices.coreinfrastructure.org/projects/7557/badge)](https://bestpractices.coreinfrastructure.org/projects/7557)
[![Code Style Black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

| Branch | Workflow Status | Code Coverage | Vulnerabilities | Bugs |
|--------|-----------------|---------------|----------|------|
| main | [![Main](https://github.com/rtdip/core/actions/workflows/main.yml/badge.svg?branch=main)](https://github.com/rtdip/core/actions/workflows/main.yml) | [![Coverage](https://sonarcloud.io/api/project_badges/measure?project=rtdip_core&metric=coverage&branch=main)](https://sonarcloud.io/summary/new_code?id=rtdip_core) | [![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=rtdip_core&metric=vulnerabilities&branch=main)](https://sonarcloud.io/summary/new_code?id=rtdip_core) | [![Bugs](https://sonarcloud.io/api/project_badges/measure?project=rtdip_core&metric=bugs&branch=main)](https://sonarcloud.io/summary/new_code?id=rtdip_core) |
| develop | [![Develop](https://github.com/rtdip/core/actions/workflows/develop.yml/badge.svg)](https://github.com/rtdip/core/actions/workflows/develop.yml) | [![Coverage](https://sonarcloud.io/api/project_badges/measure?project=rtdip_core&metric=coverage&branch=develop)](https://sonarcloud.io/summary/new_code?id=rtdip_core) | [![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=rtdip_core&metric=vulnerabilities&branch=develop)](https://sonarcloud.io/summary/new_code?id=rtdip_core) | [![Bugs](https://sonarcloud.io/api/project_badges/measure?project=rtdip_core&metric=bugs&branch=develop)](https://sonarcloud.io/summary/new_code?id=rtdip_core) |

</div>

This repository contains Real Time Data Ingestion Platform SDK functions and documentation. This README will be a developer guide to understand the repository.

## What is RTDIP SDK?

​​**Real Time Data Ingestion Platform (RTDIP) SDK** is a python software development kit built to provide users, data scientists and developers with the ability to interact with components of the Real Time Data Ingestion Platform, including:

- Building, Executing and Deploying Ingestion Pipelines
- Execution of queries on RTDIP data
- Authentication to securely interact with environments and data

See [RTDIP Documentation](https://www.rtdip.io/) for more information on how to use the SDK.

# Repository Guidelines


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
* Check previous questions and answers or ask new ones on our slack channel [**#rtdip**](https://lfenergy.slack.com/archives/C0484R9Q6A0)

### Community
* Chat with other community members by joining the **#rtdip** Slack channel. [Click here to join our slack community](https://lfenergy.slack.com/archives/C0484R9Q6A0)
