from rtdip_sdk.pipelines.forecasting.interfaces import MachineLearningInterface
import pandas as pd
import numpy as np
from sklearn.model_selection import TimeSeriesSplit

class BaseModelPipeline:

    def train(self, dataframe:pd.DataFrame):
        print("Needs to be overwritten")

    def predict(self, dataframe:pd.DataFrame):
        print("Needs to be overwritten")

    def panel_time_series_holdout(
            self,
            df: pd.DataFrame,
            *,
            id_col: str,
            time_col: str,
            test_size: int = 24,
            n_splits: int | None = None,  # if provided → rolling CV (multiple splits)
            sort: bool = True,
            random_state: int | None = None,  # seed for reproducibility (optional)
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Splits a multi–time-series DataFrame (with multiple IDs) into train_df and test_df.
        For each ID, the last `test_size` rows are used as the test set.

        Parameters
        ----------
        df : DataFrame
            Contains columns [id_col, time_col, ...].
        id_col : str
            Name of the column identifying each time series.
        time_col : str
            Name of the timestamp column (must be sortable).
        test_size : int
            Number of rows per ID to include in the test split.
        n_splits : int, optional
            If set → uses TimeSeriesSplit for rolling splits (cross-validation).
            If None → performs a simple holdout (last `test_size` rows as test).
        sort : bool
            Sorts by [id_col, time_col] to ensure chronological order.

        Returns
        -------
        train_df, test_df : DataFrames
            Contain all IDs, cleanly separated into training and test sets.
        """
        if random_state is not None:
            np.random.seed(random_state)

        if sort:
            df = df.sort_values([id_col, time_col]).reset_index(drop=True)

        train_parts, test_parts = [], []

        for _id, g in df.groupby(id_col, sort=False):
            if len(g) <= test_size:
                # skip very short series
                continue

            if n_splits is None:
                # simple holdout: last window as test
                g_train, g_test = g.iloc[:-test_size], g.iloc[-test_size:]
                train_parts.append(g_train)
                test_parts.append(g_test)
            else:
                # rolling cross-validation variant
                tscv = TimeSeriesSplit(n_splits=n_splits, test_size=test_size)
                for train_idx, test_idx in tscv.split(g):
                    # keep only the last split (current holdout)
                    g_train, g_test = g.iloc[train_idx], g.iloc[test_idx]
                train_parts.append(g_train)
                test_parts.append(g_test)

        train_df = pd.concat(train_parts, ignore_index=True)
        test_df = pd.concat(test_parts, ignore_index=True)
        return train_df, test_df

    def panel_time_series_train_val_test_split(self, df, id_col, time_col, val_size, test_size, random_state=None):
        train_val_df, test_df = self.panel_time_series_holdout(df,
                                                          id_col=id_col, time_col=time_col, test_size=test_size,
                                                          random_state=random_state)
        train_df, val_df = self.panel_time_series_holdout(train_val_df,
                                                     id_col=id_col, time_col=time_col, test_size=val_size,
                                                     random_state=random_state)
        return train_df, val_df, test_df

