# Getting started with the RTDIP SDK

This article provides a guide on how to install the RTDIP SDK. Get started by ensuring you have all the prerequisites before following the simple installation steps.

* [Prerequisites](#prerequisites)

* [Installing](#installing-the-rtdip-sdk)

## Prerequisites
There are a few things to note before using the RTDIP SDK. The following prerequisites will need to be installed on your local machine.

* Python version 3.8 >=, < 4.0 should be installed. Check which python version you have with the following command:

        python --version

    Find the latest python version [here](https://www.python.org/downloads/) and ensure your python path is set up correctly on your machine.

* Ensure your pip python version matches the python version on your machine. Check which version of pip you have installed with the following command:
    
        pip --version

    There are two ways to ensure you have the correct versions installed. Either upgrade your Python and pip install or create an environment.
 
    **Option 1**: To upgrade your version of pip use the following command:
    
        python -m pip install --upgrade pip

    **OR** 

    **Option 2**: To create an environment, you will need to create a **environment.yml** file with the following:

            name: rtdip-sdk
            channels:
            - conda-forge
            - defaults
            dependencies:
            - python==3.11
            - pip==22.0.2
            - pip:
                - rtdip-sdk

    Run the following command:

        conda env create -f environment.yml

    To update an environment previously created:

        conda env update -f environment.yml

* To use pyodbc or turbodbc python libraries, ensure that the required ODBC driver is installed as per these [instructions](https://docs.microsoft.com/en-us/azure/databricks/integrations/bi/jdbc-odbc-bi#download-the-odbc-driver)

* If you plan to use pyodbc, Microsoft Visual C++ 14.0 or greater is required. Get it from [Microsoft C++ Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/)

* To use turbodbc python library, ensure that [Boost](https://turbodbc.readthedocs.io/en/latest/pages/getting_started.html) is installed correctly.

## Installing the RTDIP SDK

RTDIP SDK is a PyPi package that can be found [here](https://pypi.org/project/rtdip-sdk/). On this page you can find the **project description**,  **release history**, **statistics**, **project links** and **maintainers**.

1\. To install the latest released version of RTDIP SDK from PyPi use the following command:

    pip install rtdip-sdk 

If you have previously installed the RTDIP SDK and would like the latest version, see below.

    pip install rtdip-sdk --upgrade

2\. Once the installation is complete you can learn how to use the SDK [here.](../sdk/rtdip-sdk-usage.md)

!!! note "Note"
    </b>If you are having trouble installing the SDK, ensure you have installed all of the prerequisites. If problems persist please see [Troubleshooting](../sdk/troubleshooting.md) for more information. Please also reach out to the RTDIP team via Issues, we are always looking to improve the SDK and value your input.<br />
