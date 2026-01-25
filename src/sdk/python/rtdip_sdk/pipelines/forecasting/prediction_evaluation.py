# Copyright 2025 RTDIP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import pandas as pd
import numpy as np

from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    mean_absolute_percentage_error,
)


def calculate_timeseries_forecasting_metrics(
    y_test: np.ndarray, y_pred: np.ndarray, negative_metrics: bool = True
) -> dict:
    """
    Calculates MAE, MSE, RMSE, MAPE and MASE for the parameter Dataframes.

    Args:
        y_test (np.ndarray): The test array
        y_pred (np.ndarray): The prediction array
        negative_metrics (bool): True: the metrics will be multiplied by -1 at the end.
                                 False: the metrics will not be multiplied at the end

    Returns:
        dict: A dictionary containing all the calculated metrics

    Raises:
        ValueError: If the dataframes have different lengths

    """

    # Basic shape guard to avoid misleading metrics on misaligned outputs.
    if len(y_test) != len(y_pred):
        raise ValueError(
            f"Prediction length ({len(y_pred)}) does not match test length ({len(y_test)}). "
            "Please check timestamp alignment and forecasting horizon."
        )

    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)
    rmse = np.sqrt(mse)

    # MAPE (filter near-zero values)
    non_zero_mask = np.abs(y_test) >= 0.1
    if np.sum(non_zero_mask) > 0:
        mape = mean_absolute_percentage_error(
            y_test[non_zero_mask], y_pred[non_zero_mask]
        )
    else:
        mape = np.nan

    # MASE (Mean Absolute Scaled Error)
    if len(y_test) > 1:
        naive_forecast = y_test[:-1]
        mae_naive = mean_absolute_error(y_test[1:], naive_forecast)
        mase = mae / mae_naive if mae_naive != 0 else mae
    else:
        mase = np.nan

    # SMAPE (Symmetric Mean Absolute Percentage Error)
    smape = (
        100
        * (
            2 * np.abs(y_test - y_pred) / (np.abs(y_test) + np.abs(y_pred) + 1e-10)
        ).mean()
    )

    # AutoGluon uses negative metrics (higher is better)
    factor = -1 if negative_metrics else 1

    metrics = {
        "MAE": factor * mae,
        "RMSE": factor * rmse,
        "MAPE": factor * mape,
        "MASE": factor * mase,
        "SMAPE": factor * smape,
    }

    return metrics


def calculate_timeseries_robustness_metrics(
    y_test: np.ndarray,
    y_pred: np.ndarray,
    negative_metrics: bool = False,
    tail_percentage: float = 0.2,
) -> dict:
    """
    Takes the tails from the input dataframes and calls calculate_timeseries_forecasting_metrics() with them

    Args:
        y_test (np.ndarray): The test array
        y_pred (np.ndarray): The prediction array
        negative_metrics (bool): True: the metrics will be multiplied by -1 at the end.
                                 False: the metrics will not be multiplied at the end
        tail_percentage (float): The length of the tail in percentages. 1 = whole dataframe
                                                                        0.5 = the second half of the dataframe
                                                                        0.1 = the last 10% of the dataframe

    Returns:
        dict: A dictionary containing all the calculated metrics for the selected tails

    """

    cut = round(len(y_test) * tail_percentage)
    y_test_r = y_test[-cut:]
    y_pred_r = y_pred[-cut:]

    metrics = calculate_timeseries_forecasting_metrics(
        y_test_r, y_pred_r, negative_metrics
    )

    robustness_metrics = {}
    for key in metrics.keys():
        robustness_metrics[key + "_r"] = metrics[key]

    return robustness_metrics
