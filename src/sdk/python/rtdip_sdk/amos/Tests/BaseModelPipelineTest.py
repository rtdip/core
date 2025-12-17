import pandas as pd
import numpy as np

import pytest
from rtdip_sdk.amos.models.BaseModelPipeline import BaseModelPipeline


def create_dummy_df():
    """Creates a small multi-series DataFrame for testing."""
    np.random.seed(0)
    data = []
    for sid in ["A", "B"]:
        timestamps = pd.date_range("2024-01-01", periods=10, freq="D")
        y = np.random.randn(10)
        feat = np.random.randn(10)
        data.append(
            pd.DataFrame(
                {"series_id": sid, "timestamp": timestamps, "y": y, "feat": feat}
            )
        )
    return pd.concat(data, ignore_index=True)


def test_panel_time_series_holdout_basic():
    """Simple sanity test for the holdout split."""
    df = create_dummy_df()
    bmp = BaseModelPipeline()
    train_df, test_df = bmp.panel_time_series_holdout(
        df, id_col="series_id", time_col="timestamp", test_size=3
    )

    # Train + Test roughly equals original size
    assert len(train_df) + len(test_df) == len(df)

    # No overlap between train and test timestamps per series
    for sid in df["series_id"].unique():
        train_times = set(train_df.loc[train_df.series_id == sid, "timestamp"])
        test_times = set(test_df.loc[test_df.series_id == sid, "timestamp"])
        assert train_times.isdisjoint(test_times)


def test_panel_time_series_train_val_test_split_basic():
    """Simple sanity test for train/val/test split."""
    df = create_dummy_df()
    bmp = BaseModelPipeline()
    train_df, val_df, test_df = bmp.panel_time_series_train_val_test_split(
        df, id_col="series_id", time_col="timestamp", val_size=2, test_size=3
    )

    # Sizes add up correctly
    total_len = len(train_df) + len(val_df) + len(test_df)
    assert total_len == len(df)

    # Ensure no overlapping timestamps per series
    for sid in df["series_id"].unique():
        sets = [
            set(train_df.loc[train_df.series_id == sid, "timestamp"]),
            set(val_df.loc[val_df.series_id == sid, "timestamp"]),
            set(test_df.loc[test_df.series_id == sid, "timestamp"]),
        ]
        assert sets[0].isdisjoint(sets[1])
        assert sets[0].isdisjoint(sets[2])
        assert sets[1].isdisjoint(sets[2])


def test_holdout_is_chronologically_correct():
    """Test must lie strictly in the future of train for each series."""
    df = create_dummy_df()
    bmp = BaseModelPipeline()
    train_df, test_df = bmp.panel_time_series_holdout(
        df, id_col="series_id", time_col="timestamp", test_size=3, sort=True
    )

    # ensure per-ID chronological separation: max(train) < min(test)
    for sid in test_df["series_id"].unique():
        tmax = train_df.loc[train_df.series_id == sid, "timestamp"].max()
        smin = test_df.loc[test_df.series_id == sid, "timestamp"].min()
        # sanity: both must exist
        assert pd.notna(tmax) and pd.notna(smin)
        # strict ordering
        assert tmax < smin, f"Train not strictly before test for series {sid}"


def test_train_val_test_are_chronologically_correct():
    df = create_dummy_df()
    bmp = BaseModelPipeline()
    train_df, val_df, test_df = bmp.panel_time_series_train_val_test_split(
        df, id_col="series_id", time_col="timestamp", val_size=2, test_size=3
    )

    for sid in df["series_id"].unique():
        # skip IDs that might have been dropped by the splitter
        if (
            sid not in set(train_df.series_id)
            or sid not in set(val_df.series_id)
            or sid not in set(test_df.series_id)
        ):
            continue
        tmax = train_df.loc[train_df.series_id == sid, "timestamp"].max()
        vmin = val_df.loc[val_df.series_id == sid, "timestamp"].min()
        vmax = val_df.loc[val_df.series_id == sid, "timestamp"].max()
        smin = test_df.loc[test_df.series_id == sid, "timestamp"].min()

        assert tmax < vmin <= vmax < smin, f"Chronology broken for series {sid}"
