import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

from src.sdk.python.rtdip_sdk.pipelines.anomaly_detection.spark.mad_anomaly_detection import (
    MadAnomalyDetection,
    MadAnomalyDetectionRollingWindow,
)


def plot_results(df_raw, df_anoms, title, ax):
    # Convert to Pandas
    pdf = df_raw.toPandas().sort_values("timestamp")
    apdf = df_anoms.toPandas().sort_values("timestamp")

    # Original TS
    ax.plot(
        pdf["timestamp"],
        pdf["value"],
        label="Values",
        color="blue",
        linewidth=1.8,
    )

    # Mark anomalies
    if len(apdf) > 0:
        ax.scatter(
            apdf["timestamp"],
            apdf["value"],
            color="red",
            s=80,
            label="Anomalies",
            zorder=5,
        )

    ax.set_title(title)
    ax.set_xlabel("Timestamp")
    ax.set_ylabel("Value")
    ax.legend()
    ax.grid(True, linestyle="--", alpha=0.4)


def main():
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("MAD Visualization")
        .getOrCreate()
    )

    # -----------------------------
    # Small dataset for simple MAD
    # -----------------------------
    data_small = [
        (1, 10.0),
        (2, 12.0),
        (3, 10.5),
        (4, 11.0),
        (5, 30.0),  # anomaly
        (6, 10.2),
        (7, 9.8),
        (8, 10.1),
        (9, 10.3),
        (10, 10.0),
    ]

    df_small = spark.createDataFrame(data_small, ["timestamp", "value"])

    # -----------------------------
    # Large trending dataset for Rolling MAD
    # -----------------------------
    data_big = [
        (1, 5.8),
        (2, 6.6),
        (3, 6.2),
        (4, 7.5),
        (5, 7.0),
        (6, 8.3),
        (7, 8.1),
        (8, 9.7),
        (9, 9.2),
        (10, 10.5),
        (11, 10.7),
        (12, 11.4),
        (13, 12.1),
        (14, 11.6),
        (15, 13.0),
        (16, 13.6),
        (17, 14.2),
        (18, 14.8),
        (19, 15.3),
        (20, 15.0),
        (21, 16.2),
        (22, 16.8),
        (23, 17.4),
        (24, 18.1),
        (25, 17.7),
        (26, 18.9),
        (27, 19.5),
        (28, 19.2),
        (29, 20.1),
        (30, 20.7),
        (31, 0.0),  # anomaly
        (32, 21.5),
        (33, 22.0),
        (34, 22.9),
        (35, 23.4),
        (36, 30.0),  # anomaly
        (37, 23.8),
        (38, 24.9),
        (39, 25.1),
        (40, 26.0),
        (41, 40.0),  # anomaly
        (42, 26.5),
        (43, 27.4),
        (44, 28.0),
        (45, 28.8),
        (46, 29.1),
        (47, 29.8),
        (48, 30.5),
        (49, 31.0),
        (50, 31.6),
    ]

    df_big = spark.createDataFrame(data_big, ["timestamp", "value"])

    # -----------------------------
    # Run the anomaly detectors
    # -----------------------------
    detector_mad = MadAnomalyDetection()
    detector_rolling = MadAnomalyDetectionRollingWindow(window_size=15)

    anoms_mad = detector_mad.detect(df_small)
    anoms_rolling = detector_rolling.detect(df_big)

    # -----------------------------
    # Visualization
    # -----------------------------
    fig, axs = plt.subplots(2, 1, figsize=(12, 10))

    plot_results(df_small, anoms_mad, "MAD Anomaly Detection", axs[0])
    plot_results(df_big, anoms_rolling, "Rolling MAD Anomaly Detection", axs[1])

    plt.tight_layout()
    plt.show()

    spark.stop()


if __name__ == "__main__":
    main()
