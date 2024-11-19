# Software Bill of Materials (SBOM)

## Project Name: rtdip-sdk
## Version: [RTDIP_SDK_1]
## Date: [05.11.2024]
## License: Apache License, Version 2.0

### Overview
This SBOM lists all required and optional dependencies for the `rtdip-sdk` project, including their versions and licenses. 

### Components


| Field | Name                           | Version Range        | Supplier                    | License           | Comment                  |
|-------|--------------------------------|----------------------|-----------------------------|--------------------|--------------------------|
| 1     | databricks-sql-connector       | >=3.1.0,<4.0.0       | Databricks, Inc.            | Apache 2.0         | SQL connector for Databricks |
| 2     | azure-identity                 | >=1.12.0,<2.0.0      | Microsoft                   | MIT                | Identity management for Azure |
| 3     | pandas                         | >=1.5.2,<2.2.0       | The Pandas Development Team  | BSD 3-Clause       | Data manipulation library |
| 4     | jinja2                         | >=3.1.4,<4.0.0       | Jinja2 Team                 | BSD 3-Clause       | Template engine for Python |
| 5     | importlib_metadata             | >=7.0.0              | PyPa                        | MIT                | Metadata for Python packages |
| 6     | semver                         | >=3.0.0,<4.0.0       | Mikhail Korobeynikov        | MIT                | Semantic versioning library |
| 7     | xlrd                           | >=2.0.1,<3.0.0       | Python Software Foundation   | MIT                | Library for reading Excel files |
| 8     | grpcio                         | >=1.48.1             | Google LLC                  | Apache 2.0         | gRPC library for Python |
| 9     | grpcio-status                  | >=1.48.1             | Google LLC                  | Apache 2.0         | gRPC status library       |
| 10    | googleapis-common-protos       | >=1.56.4             | Google LLC                  | Apache 2.0         | Common protobufs for Google APIs |
| 11    | langchain                      | >=0.2.0,<0.3.0       | Harrison Chase              | MIT                | Framework for LLMs       |
| 12    | langchain-community            | >=0.2.0,<0.3.0       | Harrison Chase              | MIT                | Community contributions to LangChain |
| 13    | openai                         | >=1.13.3,<2.0.0      | OpenAI                      | MIT                | OpenAI API client        |
| 14    | pydantic                       | >=2.6.0,<3.0.0       | Samuel Colvin               | MIT                | Data validation library   |
| 15    | pyspark                        | >=3.3.0,<3.6.0       | The Apache Software Foundation | Apache 2.0       | Spark library for Python  |
| 16    | delta-spark                    | >=2.2.0,<3.3.0       | Databricks, Inc.           | Apache 2.0         | Delta Lake integration with Spark |
| 17    | dependency-injector            | >=4.41.0,<5.0.0      | Paul Ganssle               | MIT                | Dependency injection framework |
| 18    | databricks-sdk                 | >=0.20.0,<1.0.0      | Databricks, Inc.           | Apache 2.0         | SDK for Databricks services |
| 19    | azure-storage-file-datalake    | >=12.12.0,<13.0.0    | Microsoft                   | MIT                | Azure Data Lake Storage client |
| 20    | azure-mgmt-storage             | >=21.0.0             | Microsoft                   | MIT                | Azure Storage management client |
| 21    | azure-mgmt-eventgrid           | >=10.2.0             | Microsoft                   | MIT                | Azure Event Grid management client |
| 22    | boto3                          | >=1.28.2,<2.0.0      | Amazon Web Services         | Apache 2.0         | AWS SDK for Python       |
| 23    | hvac                           | >=1.1.1              | HashiCorp                   | MPL 2.0            | HashiCorp Vault client   |
| 24    | azure-keyvault-secrets         | >=4.7.0,<5.0.0       | Microsoft                   | MIT                | Azure Key Vault secrets management |
| 25    | web3                           | >=6.18.0,<7.0.0      | N/A                         | MIT                | Ethereum blockchain library |
| 26    | polars[deltalake]              | >=0.18.8,<1.0.0      | N/A                         | MIT                | DataFrame library with Delta Lake support |
| 27    | delta-sharing                  | >=1.0.0,<1.1.0       | N/A                         | Apache 2.0         | Delta Sharing library     |
| 28    | xarray                         | >=2023.1.0,<2023.8.0 | N/A                         | BSD 3-Clause       | N-dimensional array library |
| 29    | ecmwf-api-client               | >=1.6.3,<2.0.0       | N/A                         | Apache 2.0         | ECMWF API client         |
| 30    | netCDF4                        | >=1.6.4,<2.0.0       | N/A                         | BSD 3-Clause       | NetCDF file reading/writing |
| 31    | joblib                         | >=1.3.2,<2.0.0       | N/A                         | BSD 3-Clause       | Lightweight pipelining library |
| 32    | sqlparams                      | >=5.1.0,<6.0.0       | N/A                         | MIT                | SQL query parameters library |
| 33    | entsoe-py                      | >=0.5.10,<1.0.0      | N/A                         | MIT                | ENTSOE API client        |
| 34    | pytest                         | ==7.4.0              | N/A                         | MIT                | Testing framework        |
| 35    | pytest-mock                    | ==3.11.1             | N/A                         | MIT                | Mocking for pytest       |
| 36    | pytest-cov                     | ==4.1.0              | N/A                         | MIT                | Coverage reporting for pytest |
| 37    | pylint                         | ==2.17.4             | N/A                         | GPL 2.0            | Static code analysis for Python |
| 38    | pip                            | >=23.1.2             | N/A                         | MIT                | Python package installer  |
| 39    | turbodbc                       | ==4.11.0             | N/A                         | MIT                | ODBC interface for Python |
| 40    | numpy                          | >=1.23.4,<2.0.0      | NumPy Developers            | BSD 3-Clause       | Numerical computing library |
| 41    | oauthlib                       | >=3.2.2,<4.0.0       | N/A                         | MIT                | OAuth library            |
| 42    | cryptography                   | >=38.0.3             | N/A                         | MIT                | Cryptography library     |
| 43    | fastapi                        | >=0.110.0,<1.0.0     | Sebastián Ramírez          | MIT                | Fast web framework       |
| 44    | httpx                          | >=0.24.1,<1.0.0      | N/A                         | MIT                | HTTP client for Python   |
| 45    | openjdk                        | >=11.0.15,<12.0.0    | N/A                         | N/A                | OpenJDK Java runtime     |
| 46    | mkdocs-material                | ==9.5.20             | N/A                         | MIT                | Material theme for MkDocs |
| 47    | mkdocs-material-extensions     | ==1.3.1              | N/A                         | MIT                | Extensions for MkDocs    |
| 48    | mkdocstrings                   | ==0.25.0             | N/A                         | MIT                | Documentation generation |
| 49    | mkdocstrings-python            | ==1.10.8             | N/A                         | MIT                | Python support for mkdocstrings |
| 50    | mkdocs-macros-plugin           | ==1.0.1              | N/A                         | MIT                | Macros for MkDocs        |
| 51    | mkdocs-autorefs                | >=1.0.0,<1.1.0       | N/A                         | MIT                | Automatic references for MkDocs |
| 52    | pygments                       | ==2.16.1             | N/A                         | BSD 2-Clause       | Syntax highlighting library |
| 53    | pymdown-extensions             | ==10.8.1             | N/A                         | MIT                | Extensions for Markdown   |
| 54    | pygithub                       | >=1.59.0             | N/A                         | MIT                | GitHub API client        |
| 55    | pyjwt                          | >=2.8.0,<3.0.0       | N/A                         | MIT                | JSON Web                 |
| 56    | conda                          | >=24.9.2             | N/A                         | BSD 3-Clause       | Package installer        |
| 57    | python                         | >=3.9,<3.12          | Python Software Foundation  | PSF           | Python programming language          |
| 58    | pyodbc                         | >=4.0.39,<5.0.0      | N/A                         | MIT           | ODBC library for Python              |
| 59    | twine                          | ==4.0.2              | PyPA                        | Apache 2.0    | Python package publishing tool       |
| 60    | black                          | >=24.1.0             |  Python Software Foundation  | MIT           | Code formatter for Python            |
| 61    | great-expectations             | >=0.18.8,<1.0.0      |  N/A                         | Apache 2.0    | Data validation tool                 |
| 62    | azure-functions                | >=1.15.0,<2.0.0      | Microsoft                   | MIT           | Functions for Azure services         |
| 63    | build                          | ==0.10.0             | PyPA                        | MIT           | Python package build tool            |
| 64    | deltalake                      | >=0.10.1,<1.0.0      | Delta, Inc.                 | Apache 2.0    | Delta Lake interaction for Python    |
| 65    | trio                           | >=0.22.1             | Python Software Foundation  | MIT           | Async library for concurrency        |
| 66    | eth-typing                     | >=4.2.3,<5.0.0       | Ethereum Foundation         | MIT           | Ethereum types library               |
| 67    | moto[s3]                       | >=5.0.16,<6.0.0      | Spulec                      | Apache 2.0    | Mock library for AWS S3              |
| 68    | pyarrow                        | >=14.0.1,<17.0.0     | Apache Arrow                | Apache 2.0    | Columnar data storage and processing |

### Summary
- **Total Components**: 68
- **Last Updated**: [05.11.2024]
