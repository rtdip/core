# Getting started with RTDIP

<p align="center"><img src=https://raw.githubusercontent.com/rtdip/core/develop/docs/getting-started/images/rtdip-horizontal-color.png alt="rtdip" width=50% height=50%/></p>

RTDIP provides functionality to process and query real time data. The RTDIP SDK is central to building pipelines and querying data, so getting started with it is key to unlocking the capability of RTDIP.

This article provides a guide on how to install the RTDIP SDK. Get started by ensuring you have all the prerequisites before following the simple installation steps.

* [Prerequisites](#prerequisites)

* [Installation](#installing-the-rtdip-sdk)

## Prerequisites

<!-- --8<-- [start:prerequisites] -->

### Python

There are a few things to note before using the RTDIP SDK. The following prerequisites will need to be installed on your local machine.

Python version 3.9 >= and < 3.12 should be installed. Check which python version you have with the following command:

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

<!-- --8<-- [end:prerequisites] -->

### ODBC
To use pyodbc or turbodbc python libraries, ensure it is installed as per the below and the ODBC driver is installed as per these [instructions](https://docs.microsoft.com/en-us/azure/databricks/integrations/bi/jdbc-odbc-bi#download-the-odbc-driver).

=== "Pyodbc"
    1. If you plan to use pyodbc, Microsoft Visual C++ 14.0 or greater is required. Get it from [Microsoft C++ Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/).
    1. If you are using linux, install unixodbc as per these [instructions.](https://github.com/mkleehammer/pyodbc/wiki/Install)
    1. Install the `pyodbc` python package into your python environment.

=== "Turbodbc"
    1. To use turbodbc python library, ensure to follow the [Turbodbc Getting Started](https://turbodbc.readthedocs.io/en/latest/pages/getting_started.html) section and ensure that [Boost](https://turbodbc.readthedocs.io/en/latest/pages/getting_started.html) is installed correctly. 
    1. Install the `turbodbc` python package into your python environment.

### Spark Connect

Spark Connect was released in Apache Spark 3.4.0 and enables a decoupled client-server architecture that allows remote connectivity to Spark Clusters. RTDIP SDK supports Spark Connect and can be configured using the Spark Connector and providing the Spark Connect connection string required to connect to your Spark Cluster.

Please ensure that you have followed the [instructions](https://spark.apache.org/docs/latest/spark-connect-overview.html#how-to-use-spark-connect) to enable Spark Connect on your Spark Cluster and that you are using a `pyspark>=3.4.0`. If you are connecting to a Databricks Cluster, then you may prefer to install python package `databricks-connect>=13.0.1` instead of `pyspark`.

### Java

To use RTDIP Pipelines components in your own environment that leverages [pyspark](https://spark.apache.org/docs/latest/api/python/getting_started/install.html) and you do not want to leverage [Spark Connect](#spark-connect), Java 8 or later is a [prerequisite](https://spark.apache.org/docs/latest/api/python/getting_started/install.html#dependencies). See below for suggestions to install Java in your development environment.

=== "Conda"
    A fairly simple option is to use the conda **openjdk** package to install Java into your python virtual environment. An example of a conda **environment.yml** file to achieve this is below.

    ```yaml
    name: rtdip-sdk
    channels:
        - conda-forge
        - defaults
    dependencies:
        - python==3.12
        - pip
        - openjdk==11.0.15
        - pip:
            - rtdip-sdk
    ```
    
    !!! note "Pypi"
        This package is not available from Pypi.

=== "Java"
    Follow the official Java JDK installation documentation [here.](https://docs.oracle.com/en/java/javase/11/install/overview-jdk-installation.html)

    - [Windows](https://docs.oracle.com/en/java/javase/11/install/installation-jdk-microsoft-windows-platforms.html)
    - [Mac OS](https://docs.oracle.com/en/java/javase/11/install/installation-jdk-macos.html)
    - [Linux](https://docs.oracle.com/en/java/javase/11/install/installation-jdk-linux-platforms.html)

    !!! note "Windows"
        Windows requires an additional installation of a file called **winutils.exe**. Please see this [repo](https://github.com/steveloughran/winutils) for more information.

## Installing the RTDIP SDK

<!-- --8<-- [start:installation] -->

RTDIP SDK is a PyPi package that can be found [here](https://pypi.org/project/rtdip-sdk/). On this page you can find the **project description**,  **release history**, **statistics**, **project links** and **maintainers**.

Features of the SDK can be installed using different extras statements when installing the **rtdip-sdk** package:

=== "Queries"
    When installing the package for only quering data, simply specify  in your preferred python package installer:

        pip install rtdip-sdk

=== "Pipelines"
    RTDIP SDK can be installed to include the packages required to build, execute and deploy pipelines. Specify the following extra **[pipelines]** when installing RTDIP SDK so that the required python packages are included during installation.

        pip install "rtdip-sdk[pipelines]"

=== "Pipelines + Pyspark"
    RTDIP SDK can also execute pyspark functions as a part of the pipelines functionality. Specify the following extra **[pipelines,pyspark]** when installing RTDIP SDK so that the required pyspark python packages are included during installation.

        pip install "rtdip-sdk[pipelines,pyspark]"

    !!! note "Java"
        Ensure that Java is installed prior to installing the rtdip-sdk with the **[pipelines,pyspark]**. See [here](https://www.rtdip.io/getting-started/installation/#java) for more information.

The following provides examples of how to install the RTDIP SDK package with Pip, Conda or Micromamba. Please note the section above to update any extra packages to be installed as part of the RTDIP SDK.

=== "Pip"

    To install the latest released version of RTDIP SDK from PyPi use the following command:

        pip install rtdip-sdk 

    If you have previously installed the RTDIP SDK and would like the latest version, see below:

        pip install rtdip-sdk --upgrade

=== "Conda"

    To create an environment, you will need to create a **environment.yml** file with the following:
    
    ```yaml
    name: rtdip-sdk
    channels:
        - conda-forge
        - defaults
    dependencies:
        - python==3.12
        - pip
        - pip:
            - rtdip-sdk
    ```

    Run the following command:

        conda env create -f environment.yml

    To update an environment previously created:

        conda env update -f environment.yml

=== "Micromamba" 

    To create an environment, you will need to create a **environment.yml** file with the following:

    ```yaml
    name: rtdip-sdk
    channels:
        - conda-forge
        - defaults
    dependencies:
        - python==3.12
        - pip
        - pip:
            - rtdip-sdk
    ```

    Run the following command:

        micromamba create -f environment.yml

    To update an environment previously created:

        micromamba update -f environment.yml


<!-- --8<-- [end:installation] -->

## Next steps
Once the installation is complete you can learn how to use the SDK [here.](../sdk/overview.md)

!!! note "Note"
    </b>If you are having trouble installing the SDK, ensure you have installed all of the prerequisites. If problems persist please see [Troubleshooting](../sdk/queries/databricks/troubleshooting.md) for more information. Please also reach out to the RTDIP team via Issues, we are always looking to improve the SDK and value your input.<br />
