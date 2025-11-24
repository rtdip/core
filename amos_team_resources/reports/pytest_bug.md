---
name: Bug report - Pytest 
about: SparkSession fails when additional RTDIP modules are imported (dynamic component discovery issue)
title: "SparkSession initialization fails when additional tests import RTDIP components"
labels: bug
assignees: ''
---

## **Describe the bug**

When additional test modules import RTDIP pipeline components (e.g., LSTM/XGBoost tests or any new modules), the RTDIP `SparkSessionUtility` begins discovering a different set of components during runtime. This causes the generated Spark configuration to change unpredictably.

As a result, `SparkClient` may receive an incomplete or invalid configuration (e.g., missing `spark.master`), leading to a failure to initialize `SparkSession`.  
All Spark-related tests then fail with:

```
AttributeError: 'NoneType' object has no attribute 'sc'
```

This happens **even though Spark tests pass when the new test modules are not present**.

## **To Reproduce**

1. Clone the current `develop` branch.

   
2. Run pytest on **both** test directories at the same time:

   ```bash
   pytest tests/ amos_team_resources/tests/
   ```

3. Observe that many Spark-related tests fail with:

   ```
   AttributeError: 'NoneType' object has no attribute 'sc'
   ```

4. Run pytest with **only the main test directory**:

   ```bash
   pytest tests/
   ```

   → All Spark tests pass successfully.

5. The error disappears as soon as the additional test directory is removed or excluded.

## **Expected behavior**

SparkSession initialization should be **deterministic**, independent of:

- test import order  
- which RTDIP modules happen to be imported  
- unrelated pipeline components  
- pytest discovery behavior

Adding additional tests should **not** affect the Spark configuration or component discovery pipeline.

## **Screenshots / Errors**

Example error:

```
FAILED test_xxx: AttributeError: 'NoneType' object has no attribute 'sc'
```

or

```
Py4JJavaError: An error occurred while calling [...]
```

## **Installation Setup**

- **OS:** Linux b6bc1f14afc6 6.14.0-35-generic #35~24.04.1-Ubuntu SMP PREEMPT_DYNAMIC (Devcontainer)
- **Python:** 3.12.12 (via devcontainer feature)
- **SDK Version:** RTDIP develop branch (2025-11-24)
- **Environment:** mamba + environment.yaml

## **Additional context**

RTDIP’s Spark initialization resolves Spark configuration dynamically via:

- `PipelineComponentsGetUtility`
- `SparkSessionUtility` using `inspect.stack()`
- scanning imported RTDIP components at runtime

Because pytest imports **all test modules before running tests**, adding new test files changes the import graph. This results in RTDIP discovering a different set of components and generating a different Spark configuration.

This design makes Spark initialization **import-order-dependent**, leading to nondeterministic behavior when additional modules are imported.

Recommendation:

- making SparkSession initialization deterministic  
- preventing imported modules from influencing Spark configuration  
- removing runtime component discovery in favor of explicit configuration  
