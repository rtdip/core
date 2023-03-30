# Getting started with RTDIP

<p align="center"><img src=https://raw.githubusercontent.com/rtdip/core/develop/docs/getting-started/images/rtdip-horizontal-color.png alt="rtdip" width=50% height=50%/></p>

RTDIP provides functionality to process and query real time data. The RTDIP SDK is central to building pipelines and querying data, so getting started with it is key to unlocking the capability of RTDIP.

This article provides a guide on how to install the RTDIP SDK. Get started by ensuring you have all the prerequisites before following the simple installation steps.

* [Prerequisites](#prerequisites)

* [Installation](#installing-the-rtdip-sdk)

## Prerequisites

### Python

There are a few things to note before using the RTDIP SDK. The following prerequisites will need to be installed on your local machine.

Python version 3.8 >= and < 3.11 should be installed. Check which python version you have with the following command:

    python --version

Find the latest python version [here](https://www.python.org/downloads/) and ensure your python path is set up correctly on your machine.

### Python Package Installers

Installing the RTDIP can be done using a package installer, such as [Pip](https://pip.pypa.io/en/stable/getting-started/), [Conda](https://docs.conda.io/en/latest/) or [Micromamba](https://mamba.readthedocs.io/en/latest/user_guide/micromamba.html).

=== "Pip"
    Ensure your pip python version matches the python version on your machine. Check which version of pip you have installed with the following command:
        
        pip --version

    There are two ways to ensure you have the correct versions installed. Either upgrade your Python and pip install or create an environment.
       
        python -m pip install --upgrade pip

=== "Conda"

    Check which version of Conda is installed with the following command:
        
        conda --version

    If necessary, upgrade Conda as follows:
       
        conda update conda

=== "Micromamba"

    Check which version of Micromamba is installed with the following command:
        
        micromamba --version

    If necessary, upgrade Micromamba as follows:
       
        micromamba self-update

### ODBC
To use pyodbc or turbodbc python libraries, ensure that the required ODBC driver is installed as per these [instructions](https://docs.microsoft.com/en-us/azure/databricks/integrations/bi/jdbc-odbc-bi#download-the-odbc-driver).

#### Pyodbc
If you plan to use pyodbc, Microsoft Visual C++ 14.0 or greater is required. Get it from [Microsoft C++ Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/).

#### Turbodbc
To use turbodbc python library, ensure to follow the [Turbodbc Getting Started](https://turbodbc.readthedocs.io/en/latest/pages/getting_started.html) section and ensure that [Boost](https://turbodbc.readthedocs.io/en/latest/pages/getting_started.html) is installed correctly.

## Installing the RTDIP SDK

RTDIP SDK is a PyPi package that can be found [here](https://pypi.org/project/rtdip-sdk/). On this page you can find the **project description**,  **release history**, **statistics**, **project links** and **maintainers**.

Features of the SDK can be installed using different extras statements when installing the `rtdip-sdk` package:

=== "Queries"
    When installing the package for only quering data, simply specify  in your preferred python package installer:

        rtdip-sdk

=== "Pipelines"
    When installing the package for only quering data, simply specify  in your preferred python package installer:

        rtdip-sdk[pipelines]

=== "Pipelines + Pyspark"
    When installing the package for only quering data, simply specify in your preferred python package installer:

        rtdip-sdk[pipelines,pyspark]


The following provides examples of how to install the RTDIP SDK package with Pip, Conda or Micromamba. Please note the section above to update any extra packages to be installed as part of the RTDIP SDK.

=== "Pip"

    To install the latest released version of RTDIP SDK from PyPi use the following command:

        pip install rtdip-sdk 

    If you have previously installed the RTDIP SDK and would like the latest version, see below:

        pip install rtdip-sdk --upgrade

=== "Conda"

    To create an environment, you will need to create a **environment.yml** file with the following:

        name: rtdip-sdk
        channels:
        - conda-forge
        - defaults
        dependencies:
        - python==3.10
        - pip==23.0.1
        - pip:
            - rtdip-sdk

    Run the following command:

        conda env create -f environment.yml

    To update an environment previously created:

        conda env update -f environment.yml

=== "Micromamba" 

    To create an environment, you will need to create a **environment.yml** file with the following:

        name: rtdip-sdk
        channels:
        - conda-forge
        - defaults
        dependencies:
        - python==3.10
        - pip==23.0.1
        - pip:
            - rtdip-sdk

    Run the following command:

        micromamba create -f environment.yml

    To update an environment previously created:

        micromamba update -f environment.yml

## Next steps
Once the installation is complete you can learn how to use the SDK [here.](../sdk/overview.md)

!!! note "Note"
    </b>If you are having trouble installing the SDK, ensure you have installed all of the prerequisites. If problems persist please see [Troubleshooting](../sdk/troubleshooting.md) for more information. Please also reach out to the RTDIP team via Issues, we are always looking to improve the SDK and value your input.<br />
