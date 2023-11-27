# Copyright 2023 RTDIP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys

sys.path.insert(0, ".")
import pandas as pd
import numpy as np
import pytest
import mlflow
from src.sdk.python.rtdip_sdk.integrations.openstef.serializer import MLflowSerializer
from pytest_mock import MockerFixture
from openstef.model.regressors.regressor import OpenstfRegressor
from openstef.data_classes.model_specifications import ModelSpecificationDataClass
from openstef.metrics.reporter import Report

find_models_path = "src.sdk.python.rtdip_sdk.integrations.openstef.serializer.MLflowSerializer._find_models"
os_path = "os.environ"


def test_save_model(mocker: MockerFixture):
    model = mocker.MagicMock(spec=OpenstfRegressor)
    experiment_name = "test_experiment"
    model_type = "test_model"
    model_specs = mocker.MagicMock(spec=ModelSpecificationDataClass)
    report = mocker.MagicMock(spec=Report)
    phase = "test_phase"

    mock_set_experiment = mocker.patch("mlflow.set_experiment", return_value=None)
    mock_start_run = mocker.patch("mlflow.start_run", return_value=mocker.MagicMock())
    mock_log_model_with_mlflow = mocker.patch(
        "src.sdk.python.rtdip_sdk.integrations.openstef.serializer.MLflowSerializer._log_model_with_mlflow",
        return_value=None,
    )
    mock_log_figures_with_mlflow = mocker.patch(
        "src.sdk.python.rtdip_sdk.integrations.openstef.serializer.MLflowSerializer._log_figures_with_mlflow",
        return_value=None,
    )
    mocker.patch.dict(
        os_path, {"DATABRICKS_WORKSPACE_USERNAME": "mock_username"}, clear=True
    )

    serializer = MLflowSerializer(mlflow_tracking_uri="test_uri")
    serializer.save_model(
        model=model,
        experiment_name=experiment_name,
        model_type=model_type,
        model_specs=model_specs,
        report=report,
        phase=phase,
    )

    assert mock_set_experiment.called
    assert mock_start_run.called
    assert mock_log_model_with_mlflow.called
    assert mock_log_figures_with_mlflow.called


def test_load_model(mocker: MockerFixture):
    experiment_name = "test_experiment"
    data = {"age": [1, 2, 3], "artifact_uri": [4, 5, 6]}
    models_df = pd.DataFrame(data)

    mock_find_models = mocker.patch(
        find_models_path,
        return_value=models_df,
    )
    mock_get_model_uri = mocker.patch(
        "src.sdk.python.rtdip_sdk.integrations.openstef.serializer.MLflowSerializer._get_model_uri",
        return_value="file://test_uri",
    )
    mock_load_model = mocker.patch(
        "mlflow.sklearn.load_model", return_value=models_df.iloc[0]
    )
    mock_determine_model_age_from_mlflow_run = mocker.patch(
        "src.sdk.python.rtdip_sdk.integrations.openstef.serializer.MLflowSerializer._determine_model_age_from_mlflow_run",
        return_value=None,
    )
    mock_get_model_specs = mocker.patch(
        "src.sdk.python.rtdip_sdk.integrations.openstef.serializer.MLflowSerializer._get_model_specs",
        return_value=None,
    )
    mocker.patch.dict(
        os_path, {"DATABRICKS_WORKSPACE_USERNAME": "mock_username"}, clear=True
    )

    serializer = MLflowSerializer(mlflow_tracking_uri="test_uri")
    serializer.load_model(experiment_name=experiment_name)

    assert mock_load_model.called
    assert mock_find_models.called
    assert mock_get_model_uri.called
    assert mock_determine_model_age_from_mlflow_run.called
    assert mock_get_model_specs.called


def test_load_model_fails(mocker: MockerFixture):
    experiment_name = "test_experiment"

    mocker.patch(
        find_models_path,
        return_value=pd.DataFrame(),
    )
    mocker.patch.dict(
        os_path, {"DATABRICKS_WORKSPACE_USERNAME": "mock_username"}, clear=True
    )

    serializer = MLflowSerializer(mlflow_tracking_uri="test_uri")

    with pytest.raises(LookupError) as e:
        serializer.load_model(experiment_name=experiment_name)

    assert str(e.value) == "Model not found. First train a model!"


def test_get_model_age(mocker: MockerFixture):
    experiment_name = "test_experiment"
    data = {"age": [1, 2, 3], "artifact_uri": [4, 5, 6]}
    models_df = pd.DataFrame(data)

    mock_find_models = mocker.patch(
        find_models_path,
        return_value=models_df,
    )
    mock_determine_model_age_from_mlflow_run = mocker.patch(
        "src.sdk.python.rtdip_sdk.integrations.openstef.serializer.MLflowSerializer._determine_model_age_from_mlflow_run",
        return_value=None,
    )
    mocker.patch.dict(
        os_path, {"DATABRICKS_WORKSPACE_USERNAME": "mock_username"}, clear=True
    )

    serializer = MLflowSerializer(mlflow_tracking_uri="test_uri")
    serializer.get_model_age(experiment_name=experiment_name)

    assert mock_find_models.called
    assert mock_determine_model_age_from_mlflow_run.called


def test_get_model_age_empty(mocker: MockerFixture):
    experiment_name = "test_experiment"

    mock_find_models = mocker.patch(
        find_models_path,
        return_value=pd.DataFrame(),
    )
    mocker.patch.dict(
        os_path, {"DATABRICKS_WORKSPACE_USERNAME": "mock_username"}, clear=True
    )

    serializer = MLflowSerializer(mlflow_tracking_uri="test_uri")
    age = serializer.get_model_age(experiment_name=experiment_name)

    assert mock_find_models.called
    assert age == np.inf


def test_remove_old_models(mocker: MockerFixture):
    experiment_name = "test_experiment"
    data = {
        "age": [1, 2, 3, 4],
        "artifact_uri": [5, 6, 7, 8],
        "end_time": pd.date_range(start="2022-01-01", periods=4, freq="D"),
        "run_id": ["1", "2", "3", "4"],
    }
    models_df = pd.DataFrame(data)

    mock_find_models = mocker.patch(
        find_models_path,
        return_value=models_df,
    )
    mock_delete_run = mocker.patch("mlflow.delete_run", return_value=None)
    mock_get_run = mocker.patch("mlflow.get_run", return_value=mocker.MagicMock())
    mock_get_artifact_repository = mocker.patch(
        "src.sdk.python.rtdip_sdk.integrations.openstef.serializer.get_artifact_repository",
        return_value=mocker.MagicMock(),
    )
    mocker.patch.dict(
        os_path, {"DATABRICKS_WORKSPACE_USERNAME": "mock_username"}, clear=True
    )

    serializer = MLflowSerializer(mlflow_tracking_uri="test_uri")
    serializer.remove_old_models(experiment_name=experiment_name, max_n_models=2)

    assert mock_find_models.called
    assert mock_delete_run.called
    assert mock_get_run.called
    assert mock_get_artifact_repository.called


def test_remove_old_models_fails(mocker: MockerFixture):
    experiment_name = "test_experiment"
    data = {
        "age": [1, 2, 3, 4],
        "artifact_uri": [5, 6, 7, 8],
        "end_time": pd.date_range(start="2022-01-01", periods=4, freq="D"),
        "run_id": ["1", "2", "3", "4"],
    }
    models_df = pd.DataFrame(data)

    mocker.patch(
        find_models_path,
        return_value=models_df,
    )
    mocker.patch("mlflow.delete_run", return_value=None)
    mocker.patch("mlflow.get_run", return_value=mocker.MagicMock())
    mocker.patch(
        "src.sdk.python.rtdip_sdk.integrations.openstef.serializer.get_artifact_repository",
        side_effect=Exception,
    )
    mocker.patch.dict(
        os_path, {"DATABRICKS_WORKSPACE_USERNAME": "mock_username"}, clear=True
    )

    serializer = MLflowSerializer(mlflow_tracking_uri="test_uri")

    with pytest.raises(Exception):
        serializer.remove_old_models(experiment_name=experiment_name, max_n_models=2)
