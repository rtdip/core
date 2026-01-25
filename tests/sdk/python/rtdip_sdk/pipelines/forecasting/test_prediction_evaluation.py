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

"""
LSTM-based time series forecasting implementation for RTDIP.

This module provides an LSTM neural network implementation for multivariate
time series forecasting using TensorFlow/Keras with sensor embeddings.
"""

import numpy as np
import pytest

from src.sdk.python.rtdip_sdk.pipelines.forecasting.prediction_evaluation import (
    calculate_timeseries_forecasting_metrics,
    calculate_timeseries_robustness_metrics,
)


@pytest.fixture(scope="function")
def simple_series():
    """
    Creates a small deterministic series for metric validation.
    """
    y_test = np.array([1.0, 2.0, 3.0, 4.0], dtype=float)
    y_pred = np.array([1.5, 1.5, 3.5, 3.5], dtype=float)
    return y_test, y_pred


@pytest.fixture(scope="function")
def near_zero_series():
    """
    Creates a series where all y_test values are near zero (< 0.1) to validate MAPE behavior.
    """
    y_test = np.array([0.0, 0.05, -0.09], dtype=float)
    y_pred = np.array([0.01, 0.04, -0.1], dtype=float)
    return y_test, y_pred


def test_forecasting_metrics_length_mismatch_raises():
    """
    Test that a length mismatch raises a ValueError with a helpful message.
    """
    y_test = np.array([1.0, 2.0, 3.0], dtype=float)
    y_pred = np.array([1.0, 2.0], dtype=float)

    with pytest.raises(
        ValueError, match="Prediction length .* does not match test length"
    ):
        calculate_timeseries_forecasting_metrics(y_test=y_test, y_pred=y_pred)


def test_forecasting_metrics_keys_present(simple_series):
    """
    Test that all expected metric keys exist.
    """
    y_test, y_pred = simple_series
    metrics = calculate_timeseries_forecasting_metrics(
        y_test=y_test, y_pred=y_pred, negative_metrics=True
    )

    for key in ["MAE", "RMSE", "MAPE", "MASE", "SMAPE"]:
        assert key in metrics, f"Missing metric key: {key}"


def test_forecasting_metrics_negative_flag_flips_sign(simple_series):
    """
    Test that negative_metrics flips the sign of all returned metrics.
    """
    y_test, y_pred = simple_series

    m_pos = calculate_timeseries_forecasting_metrics(
        y_test=y_test, y_pred=y_pred, negative_metrics=False
    )
    m_neg = calculate_timeseries_forecasting_metrics(
        y_test=y_test, y_pred=y_pred, negative_metrics=True
    )

    for k in ["MAE", "RMSE", "MAPE", "MASE", "SMAPE"]:
        if np.isnan(m_pos[k]):
            assert np.isnan(m_neg[k])
        else:
            assert np.isclose(m_neg[k], -m_pos[k]), f"Metric {k} should be sign-flipped"


def test_forecasting_metrics_known_values(simple_series):
    """
    Test metrics against hand-checked expected values for a simple example.
    """
    y_test, y_pred = simple_series

    # Errors: [0.5, 0.5, 0.5, 0.5]
    expected_mae = 0.5
    # MSE: mean([0.25, 0.25, 0.25, 0.25]) = 0.25, RMSE = 0.5
    expected_rmse = 0.5
    # Naive forecast MAE for y_test[1:] vs y_test[:-1]:
    # |2-1|=1, |3-2|=1, |4-3|=1 => mae_naive=1 => mase = 0.5/1 = 0.5
    expected_mase = 0.5

    metrics = calculate_timeseries_forecasting_metrics(
        y_test=y_test, y_pred=y_pred, negative_metrics=False
    )

    assert np.isclose(metrics["MAE"], expected_mae)
    assert np.isclose(metrics["RMSE"], expected_rmse)
    assert np.isclose(metrics["MASE"], expected_mase)

    # MAPE should be finite here (no near-zero y_test values)
    assert np.isfinite(metrics["MAPE"])
    # SMAPE is in percent and should be > 0
    assert metrics["SMAPE"] > 0


def test_forecasting_metrics_mape_all_near_zero_returns_nan(near_zero_series):
    """
    Test that MAPE returns NaN when all y_test values are filtered out by the near-zero mask.
    """
    y_test, y_pred = near_zero_series
    metrics = calculate_timeseries_forecasting_metrics(
        y_test=y_test, y_pred=y_pred, negative_metrics=False
    )

    assert np.isnan(
        metrics["MAPE"]
    ), "MAPE should be NaN when all y_test values are near zero"
    # The other metrics should still be computed (finite) for this case
    assert np.isfinite(metrics["MAE"])
    assert np.isfinite(metrics["RMSE"])
    assert np.isfinite(metrics["SMAPE"])


def test_forecasting_metrics_single_point_mase_is_nan():
    """
    Test that MASE is NaN when y_test has length 1.
    """
    y_test = np.array([10.0], dtype=float)
    y_pred = np.array([11.0], dtype=float)

    metrics = calculate_timeseries_forecasting_metrics(
        y_test=y_test, y_pred=y_pred, negative_metrics=False
    )
    assert np.isnan(metrics["MASE"]), "MASE should be NaN for single-point series"
    # SMAPE should be finite
    assert np.isfinite(metrics["SMAPE"])


def test_forecasting_metrics_mase_fallback_when_naive_mae_zero():
    """
    Test that MASE falls back to MAE when mae_naive == 0.
    This happens when y_test is constant (naive forecast is perfect).
    """
    y_test = np.array([5.0, 5.0, 5.0, 5.0], dtype=float)
    y_pred = np.array([6.0, 4.0, 5.0, 5.0], dtype=float)

    metrics = calculate_timeseries_forecasting_metrics(
        y_test=y_test, y_pred=y_pred, negative_metrics=False
    )
    assert np.isclose(
        metrics["MASE"], metrics["MAE"]
    ), "MASE should equal MAE when naive MAE is zero"


def test_robustness_metrics_suffix_and_values(simple_series):
    """
    Test that robustness metrics use the _r suffix and match metrics computed on the tail slice.
    """
    y_test, y_pred = simple_series
    tail_percentage = 0.5  # last half => last 2 points

    r_metrics = calculate_timeseries_robustness_metrics(
        y_test=y_test,
        y_pred=y_pred,
        negative_metrics=False,
        tail_percentage=tail_percentage,
    )

    for key in ["MAE_r", "RMSE_r", "MAPE_r", "MASE_r", "SMAPE_r"]:
        assert key in r_metrics, f"Missing robustness metric key: {key}"

    cut = round(len(y_test) * tail_percentage)
    expected = calculate_timeseries_forecasting_metrics(
        y_test=y_test[-cut:],
        y_pred=y_pred[-cut:],
        negative_metrics=False,
    )

    for k, v in expected.items():
        rk = f"{k}_r"
        if np.isnan(v):
            assert np.isnan(r_metrics[rk])
        else:
            assert np.isclose(r_metrics[rk], v), f"{rk} should match tail-computed {k}"


def test_robustness_metrics_tail_percentage_one_matches_full(simple_series):
    """
    Test that tail_percentage=1 uses the whole series and matches forecasting metrics.
    """
    y_test, y_pred = simple_series

    full = calculate_timeseries_forecasting_metrics(
        y_test=y_test, y_pred=y_pred, negative_metrics=False
    )
    r_full = calculate_timeseries_robustness_metrics(
        y_test=y_test, y_pred=y_pred, negative_metrics=False, tail_percentage=1.0
    )

    for k, v in full.items():
        rk = f"{k}_r"
        if np.isnan(v):
            assert np.isnan(r_full[rk])
        else:
            assert np.isclose(r_full[rk], v)
