from pyspark.sql import DataFrame
import pyspark.ml as ml
from ..interfaces import MachineLearningInterface
from ..._pipeline_utils.models import Libraries, SystemType


class LinearRegression(MachineLearningInterface):
    """
    This function uses pyspark.ml.LinearRegression to train a linear regression model on time data.
    And the uses the model to predict next values in the time series.

    Args:
        df (pyspark.sql.Dataframe): DataFrame containing the features and labels.
        features_col (str): Name of the column containing the features (the input). Default is 'features'.
        label_col (str): Name of the column containing the features (the input). Default is 'features'.
        prediction_col (str): Name of the column to which the prediction will be written. Default is 'prediction'.
    Returns:
        PySparkDataFrame: Returns the original PySpark DataFrame without changes.
    """

    def __init__(
        self,
        df: DataFrame,
        features_col: str = "features",
        label_col: str = "label",
        prediction_col: str = "prediction",
    ) -> None:
        self.df = df
        self.features_col = features_col
        self.label_col = label_col
        self.prediction_col = prediction_col

    @staticmethod
    def system_type():
        """
        Attributes:
            SystemType (Environment): Requires PYSPARK
        """
        return SystemType.PYSPARK

    @staticmethod
    def libraries():
        libraries = Libraries()
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def train(self):
        """
        Trains a linear regression model on the provided data.
        """
        linear_regression = ml.regression.LinearRegression(
            featuresCol=self.features_col,
            labelCol=self.label_col,
            predictionCol=self.prediction_col,
        )

        self.model = linear_regression.fit(self.df)
        return self

    def predict(self, prediction_df: DataFrame):
        """
        Predicts the next values in the time series.
        """

        return self.model.transform(
            prediction_df,
        )
