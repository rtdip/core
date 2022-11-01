# How to contribute

This Real Time Data Ingestion Platform (RTDIP) welcomes contributions and suggestions. If you have a suggestion that would improve RTDIP you can simply open an issue with the relevant title. A few contribution examples can be:

* File a bug report
* Suggest a new feature
* General enquiries 
* Security Vulnerabilities

## Prerequisites:

- Read RTDIP's [Code Of Conduct](https://github.com/rtdip/core/blob/develop/CODE_OF_CONDUCT.md) before making any contributions or suggestions.
- Install [Visual Studio Code](https://code.visualstudio.com/) and make sure you have the latest updates.
- Install [Python](https://www.python.org/downloads/). The Python version requirement is specfied in the setup.py file.
- Install [Conda](https://conda.io/projects/conda/en/latest/user-guide/install/index.html)
- Clone the github repository 
   ```sh
   git clone https://github.com/rtdip/core.git
   ```
## Issues Guidelines

To contribute to Real Time Data Ingestion Platform, open an issue by following the steps below:

1) Locate to the [Issues](https://github.com/rtdip/core/issues) tab on the github repository.
2) Select `New Issue`
3) Fill out the details and press `Submit New Issue`. 

Template examples for bug reporting and feature requests can be found [here](https://github.com/rtdip/core/tree/develop/.github/ISSUE_TEMPLATE).

The title of your Issue should be a short description of your `bug` or `suggested new feature` followed by a comment for more details.
  * *Examples of good Issue titles*
      * Added deprecationDetails config to MetricsHistogram
      * Fixed MetricsHistogram chart title tests
      * Added MR job to export data to redshift
  * *Examples of poor Issue titles*
      * Fixed tests
      * tests
      * feature/00001

## Branch Naming Convention

- Feature branches ```feature/<Issue Number>``` e.g. ```feature/12345``` - ***Issue Number*** is the number of the issue item or work item in the GitHub issues page or project board. The issue number should be 5 digits, ***For example: if your issue number is 7 then your feature branch will be feature/00007***.
- Hotfix branches ```hotfix/<Issue Number>``` e.g. ```hotfix/52679``` - ***Issue Number*** is the number of Bug/Issue Item in the GitHub project board. Hotfix branches are used for patch releases.
The standard process for working off the `develop` branch should go as follows:
1. `git fetch` in your current local branch to get latest remote branches list
2. `git checkout develop` to switch to `develop` branch
3. `git pull` to get the latest of `develop` branch
4. `git checkout -b branch_name` to create local branch for your task/issue
   * This is consistent with the Git Flow approach and with Azure DevOps integration requirements
5. Go through the motions of development and committing and pushing as before. Once completed, create a pull request into `develop` branch
6. When pull request is completed, repeat from the top to start a new local branch/new dev work item

## Testing

 * Always write tests for any parts of the code base you have touched. These tests should cover all possible edge cases (malformed input, guarding against nulls, infinite loops etc)

## How To Start Contributing/Developing

Before you start developing code, you will need to ensure you are following this respository's guidelines and standards. RTDIP SDK resides in the `src/sdk/python` folder.

1) The `src/sdk` folder is the home of all our source code. If you would like to contribute to the code base this would be our entry point. From here you can navigate to the relevant folder or create new one where needed.

> **_NOTE:_**  Our folder naming standards are all lowercase with words split by "_".

2) Write your code.

3) Write unit tests for each function. All unit tests are located in the `test/sdk` folder. The folder structure for `test` should be identical to the `src` folder. All RTDIP SDK tests are written using `pytest`.

4) All functions will need to be documented via docstrings. An example is shown below. 

    ``` 
    ```including a description of your function

    Args: any arguments your function will need

    Attributes: if needed

    Returns: what the function returns```

    ```
5) Add function documentation to the `docs/sdk/code-reference` folder using the following example:

    ```
    # Title of your function
    ::: src.sdk.{sdk_language_folder_location}.{sdk_folder_location}.{function_file}.{function_method}
    ```

> **_NOTE:_**  You will need to change the parameters respectively and remember to create docs for each function you create. RTDIP SDK uses mkdocstrings for code documentation. For more information on mkdocstrings, [see here](https://mkdocstrings.github.io/).

6) If you would like your documentation to be visible on our [RTDIP Documentation](https://www.rtdip.io/) site then you will need to add references to [mkdocs.yml](mkdocs.yml) under the **nav:** section.

7) Finally, you are ready to publish your changes. Create a PR on [Github - Pull Request](https://github.com/rtdip/core/pulls) and ensure to add reviewers from the [Codeowners List](CODEOWNERS.md) to review your code. RTDIP has built in workflows with sonarqube enabled to avoid pushing untested code. Reviewers will need to wait for all testing to pass before approving a PR and Squash Merging to develop. If everything passes your code should merge to develop successfully. 

> **_NOTE:_** Please ensure you read the [Release Guidelines](RELEASE.md) before publishing your contributions. 

8) <mark>**VERY IMPORTANT STEP!**</mark> To publish a new version of this python package you **MUST** create a tag and release in Github using the following versioning convention:

    Bump versioning standards:
    * Patch -  if you are patching code then you should only increment the last number 0.0.<mark> 1 </mark>.

    * Feature - if you are adding a new feature to develop you should increment the second number 0.<mark> 1 </mark>.0.

    * Production - if you are deploying a production ready version you should increment the first number <mark> 1 </mark>.0.0

**Always** ensure you have followed and checked all the steps to create a new release by following the [Release Guide](RELEASE.md).

RTDIP has built in pipeline workflows to automatically deploy the python package to develop or production based on your Pull Request.

Congratulations! Your clean and tested code should now be visible on Real Time Data Ingestion Platform. Thank you for your contribution. Any feedback on your experience will be greatly appreciated.