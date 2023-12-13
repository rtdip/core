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
from mlflow import ActiveRun
from mlflow.entities import Experiment
from pytest_mock import MockerFixture
from openstef.model.regressors.regressor import OpenstfRegressor
from openstef.metrics.reporter import Report, Figure, ModelSignature
from openstef.data_classes.model_specifications import ModelSpecificationDataClass
from src.sdk.python.rtdip_sdk.integrations.openstef.serializer import MLflowSerializer

experiment_name = "test_experiment"
search_runs_path = "mlflow.search_runs"
model_type = "test_model"
os_path = "os.environ"
phase = "test_phase"


def test_save_model(mocker: MockerFixture, caplog):
    model = mocker.MagicMock(spec=OpenstfRegressor)
    models_df = mocker.MagicMock(spec=pd.DataFrame)
    models_df.empty = False

    model_specs = mocker.MagicMock(spec=ModelSpecificationDataClass)
    model_specs.hyper_params = {}
    model_specs.feature_names = ["test", "name"]
    model_specs.feature_modules = []

    report = mocker.MagicMock(spec=Report)
    report.feature_importance_figure = mocker.MagicMock(spec=Figure)
    report.data_series_figures = {"test": mocker.MagicMock(spec=Figure)}
    report.signature = mocker.MagicMock(spec=ModelSignature)
    report.metrics = {}

    mocked_set_experiment = mocker.patch(
        "mlflow.set_experiment", return_value=mocker.MagicMock(spec=Experiment)
    )
    mocked_start_run = mocker.patch(
        "mlflow.start_run", return_value=mocker.MagicMock(spec=ActiveRun)
    )
    mocked_search_runs = mocker.patch(search_runs_path, return_value=models_df)
    mocked_active_run = mocker.patch(
        "mlflow.active_run", return_value=mocker.MagicMock(spec=ActiveRun)
    )
    mocked_set_tag = mocker.patch("mlflow.set_tag", return_value=None)
    mocked_log_metrics = mocker.patch("mlflow.log_metrics", return_value=None)
    mocked_log_params = mocker.patch("mlflow.log_params", return_value=None)
    mocked_log_figure = mocker.patch("mlflow.log_figure", return_value=None)
    mocked_log_model = mocker.patch("mlflow.sklearn.log_model", return_value=None)

    mocker.patch.dict(
        os_path, {"DATABRICKS_WORKSPACE_PATH": "mock_username"}, clear=True
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

    mocked_set_experiment.assert_called_once_with(
        experiment_name="mock_username" + experiment_name
    )
    mocked_start_run.assert_called_once()
    mocked_search_runs.assert_called_once()
    mocked_active_run.assert_called_once()
    mocked_log_metrics.assert_called_once()
    mocked_log_params.assert_called_once()
    mocked_log_model.assert_called_once()
    assert mocked_set_tag.call_count == 8
    assert mocked_log_figure.call_count == 2
    assert "No previous model found in MLflow" not in caplog.text
    assert "Model saved with MLflow" in caplog.text
    assert "Logged figures to MLflow." in caplog.text


def test_load_model(mocker: MockerFixture):  # write a fail test for empty model
    latest_run = mocker.MagicMock(spec=pd.DataFrame)
    latest_run.empty = False

    mock_iloc = mocker.MagicMock()
    mock_iloc.artifact_uri = "test_uri"
    mock_iloc.age = "test_age"
    latest_run.iloc.__getitem__.return_value = mock_iloc

    run = mocker.MagicMock(spec=pd.Series)
    run.end_time = pd.Timestamp("2022-01-01")

    mocked_find_models = mocker.patch(search_runs_path, return_value=latest_run)
    model_uri_spy = mocker.spy(MLflowSerializer, "_get_model_uri")
    mocked_load_model = mocker.patch(
        "src.sdk.python.rtdip_sdk.integrations.openstef.serializer.mlflow_load_model",
        return_value=run,
    )
    determine_model_age_spy = mocker.spy(
        MLflowSerializer, "_determine_model_age_from_mlflow_run"
    )
    mocked_get_model_specs = mocker.patch(
        "src.sdk.python.rtdip_sdk.integrations.openstef.serializer.MLflowSerializer._get_model_specs",
        return_value=mocker.MagicMock(spec=ModelSpecificationDataClass),
    )
    mocker.patch.dict(
        os_path, {"DATABRICKS_WORKSPACE_PATH": "mock_username"}, clear=True
    )

    serializer = MLflowSerializer(mlflow_tracking_uri="test_uri")
    serializer.load_model(experiment_name=experiment_name)

    mocked_load_model.assert_called_once()
    mocked_find_models.assert_called_once()
    model_uri_spy.assert_called_with(mocker.ANY, "test_uri")  # DO THIS
    determine_model_age_spy.assert_called_with(mocker.ANY, mock_iloc)
    mocked_get_model_specs.assert_called_once()
    assert isinstance(mocked_get_model_specs.return_value, ModelSpecificationDataClass)


def test_load_model_fails(mocker: MockerFixture):
    latest_run = mocker.MagicMock(spec=pd.DataFrame)
    latest_run.empty = True

    mocker.patch(search_runs_path, return_value=latest_run)
    mocker.patch.dict(
        os_path, {"DATABRICKS_WORKSPACE_PATH": "mock_username"}, clear=True
    )

    serializer = MLflowSerializer(mlflow_tracking_uri="test_uri")

    with pytest.raises(LookupError) as e:
        serializer.load_model(experiment_name=experiment_name)

    assert str(e.value) == "Model not found. First train a model!"


def test_get_model_age(mocker: MockerFixture, caplog):
    latest_run = mocker.MagicMock(spec=pd.DataFrame)
    latest_run.empty = False

    mock_iloc = mocker.MagicMock()
    mock_iloc.artifact_uri = "test_uri"
    mock_iloc.age = "test_age"
    latest_run.iloc.__getitem__.return_value = mock_iloc

    mocked_find_models = mocker.patch(search_runs_path, return_value=latest_run)
    determine_model_age_spy = mocker.spy(
        MLflowSerializer, "_determine_model_age_from_mlflow_run"
    )
    mocker.patch.dict(
        os_path, {"DATABRICKS_WORKSPACE_PATH": "mock_username"}, clear=True
    )

    serializer = MLflowSerializer(mlflow_tracking_uri="test_uri")
    serializer.get_model_age(experiment_name=experiment_name)

    mocked_find_models.assert_called_once()
    determine_model_age_spy.assert_called_with(mocker.ANY, mock_iloc)
    assert "No model found returning infinite model age!" not in caplog.text


def test_get_model_age_empty(mocker: MockerFixture, caplog):
    latest_run = mocker.MagicMock(spec=pd.DataFrame)
    latest_run.empty = True

    mocked_find_models = mocker.patch(search_runs_path, return_value=latest_run)
    mocker.patch.dict(
        os_path, {"DATABRICKS_WORKSPACE_PATH": "mock_username"}, clear=True
    )

    serializer = MLflowSerializer(mlflow_tracking_uri="test_uri")
    age = serializer.get_model_age(experiment_name=experiment_name)

    mocked_find_models.assert_called_once()
    assert age == np.inf
    assert "No model found returning infinite model age!" in caplog.text


def test_remove_old_models(mocker: MockerFixture):
    data = {
        "age": [1, 2, 3, 4],
        "artifact_uri": [5, 6, 7, 8],
        "end_time": pd.date_range(start="2022-01-01", periods=4, freq="D"),
        "run_id": ["1", "2", "3", "4"],
    }
    latest_run = pd.DataFrame(data)

    mocked_find_models = mocker.patch(search_runs_path, return_value=latest_run)
    mocked_delete_run = mocker.patch("mlflow.delete_run", return_value=None)
    mocked_get_run = mocker.patch("mlflow.get_run", return_value=mocker.MagicMock())
    mocked_get_artifact_repository = mocker.patch(
        "src.sdk.python.rtdip_sdk.integrations.openstef.serializer.get_artifact_repository",
        return_value=mocker.MagicMock(),
    )
    mocker.patch.dict(
        os_path, {"DATABRICKS_WORKSPACE_PATH": "mock_username"}, clear=True
    )

    serializer = MLflowSerializer(mlflow_tracking_uri="test_uri")
    serializer.remove_old_models(experiment_name=experiment_name, max_n_models=2)

    mocked_find_models.assert_called_once()
    mocked_delete_run.assert_called()
    mocked_get_run.assert_called()
    mocked_get_artifact_repository.assert_called()


def test_remove_old_models_fails(mocker: MockerFixture, caplog):
    data = {
        "age": [1, 2, 3, 4],
        "artifact_uri": [5, 6, 7, 8],
        "end_time": pd.date_range(start="2022-01-01", periods=4, freq="D"),
        "run_id": ["1", "2", "3", "4"],
    }
    latest_run = pd.DataFrame(data)

    mocker.patch(search_runs_path, return_value=latest_run)
    mocker.patch("mlflow.delete_run", return_value=None)
    mocker.patch("mlflow.get_run", return_value=mocker.MagicMock())
    mocker.patch(
        "src.sdk.python.rtdip_sdk.integrations.openstef.serializer.get_artifact_repository",
        side_effect=Exception,
    )
    mocker.patch.dict(
        os_path, {"DATABRICKS_WORKSPACE_PATH": "mock_username"}, clear=True
    )

    serializer = MLflowSerializer(mlflow_tracking_uri="test_uri")

    with pytest.raises(Exception):
        serializer.remove_old_models(experiment_name=experiment_name, max_n_models=2)

    assert "Removed artifacts" not in caplog.text
