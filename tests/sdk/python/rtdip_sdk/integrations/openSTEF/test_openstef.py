import sys

sys.path.insert(0, ".")
from pydantic.v1 import BaseSettings
from typing import Union
from pytest_mock import MockerFixture
from datetime import datetime
from src.sdk.python.rtdip_sdk.integrations.openSTEF.interfaces import _DataInterface
from src.sdk.python.rtdip_sdk.integrations.openSTEF.database import DataBase
from openstef.data_classes.prediction_job import PredictionJobDataClass
from openstef.enums import PipelineType
from src.sdk.python.rtdip_sdk.pipelines.sources import (
    PythonEntsoeSource,
    PythonMFFBASSource,
)

import pandas as pd


class ConfigSettigns(BaseSettings):
    api_username: str = "None"
    api_password: str = "None"
    api_admin_username: str = "None"
    api_admin_password: str = "None"
    api_url: str = "None"
    pcdm_host: str = "None"
    pcdm_token: str = "None"
    pcdm_port: int = 443
    pcdm_http_path: str = "None"
    pcdm_catalog: str = "rtdip"
    pcdm_schema: str = "openstef"
    db_host: str = "None"
    db_token: str = "None"
    db_port: int = 443
    db_http_path: str = "None"
    db_catalog: str = "rtdip"
    db_schema: str = "sensors"
    proxies: Union[dict[str, str], None] = None


config = ConfigSettigns()


def test_methods(mocker: MockerFixture):
    db = DataBase(config)

    mocker.patch("openstef_dbc.data_interface._DataInterface", new=_DataInterface)

    pj = PredictionJobDataClass(
        id=321,
        model="xgb",
        forecast_type="demand",
        horizon_minutes=2880,
        resolution_minutes=15,
        lat=51.813,
        lon=5.837,
        name="Location_B",
        train_components=True,
        description="Location_A_System_1+Location_A_System_2",
        quantiles=[0.05, 0.1, 0.3, 0.5, 0.7, 0.9, 0.95],
        train_split_func=None,
        backtest_split_func=None,
        train_horizons_minutes=None,
        default_modelspecs=None,
        save_train_forecasts=False,
        completeness_threshold=0.5,
        minimal_table_length=100,
        flatliner_threshold_minutes=360,
        depends_on=None,
        sid=None,
        turbine_type=None,
        n_turbines=None,
        hub_height=5,
        pipelines_to_run=[
            PipelineType.TRAIN,
            PipelineType.HYPER_PARMATERS,
            PipelineType.FORECAST,
        ],
        alternative_forecast_model_pid=None,
        data_prep_class=None,
    )

    ##### WRITE METHODS
    # data = {
    #     "input_city": ["London", "Paris", "Barcelona", "Rome"],
    #     "temp": [25, 26, 27, 28.9],
    #     "windspeed": [97, 98, 99, 99.9],
    # }
    # date_strings = [
    #     "2023-10-01T12:00:00",
    #     "2023-10-02T12:00:00",
    #     "2023-10-03T12:00:00",
    #     "2023-10-04T12:00:00",
    # ]
    # date_index = pd.to_datetime(date_strings)
    # df = pd.DataFrame(data, index=date_index)
    # x = db.write_weather_data(data=df, source="waterloo")

    # data = {"output": [1.21, 1.21, 1.21, 1.21]}
    # date_strings = [
    #     "2023-10-01T12:00:00",
    #     "2023-10-02T12:00:00",
    #     "2023-10-03T12:00:00",
    #     "2023-10-04T12:00:00",
    # ]
    # date_index = pd.to_datetime(date_strings)
    # df = pd.DataFrame(data, index=date_index)
    # x = db.write_realised(df, "Location_A_System_1")

    # data = {
    #     "output": [1.21, 1.21, 1.21, 1.21],
    #     "system": [
    #         "Location_F_System_1",
    #         "Location_A_System_1",
    #         "Location_A_System_1",
    #         "Location_A_System_1",
    #     ],
    # }
    # date_strings = [
    #     "2023-10-01T12:00:00",
    #     "2023-10-02T12:00:00",
    #     "2023-10-03T12:00:00",
    #     "2023-10-04T12:00:00",
    # ]
    # date_index = pd.to_datetime(date_strings)
    # df = pd.DataFrame(data, index=date_index)
    # x = db.write_realised_pvdata(df=df, region="")

    # x = db.write_kpi()

    # df = pd.read_csv(
    #     "/Users/Rodalyn.Barce/Documents/RTDIP/openstef/prediction.csv", index_col=0
    # )
    # df.index = pd.to_datetime(df.index)
    # x = db.write_forecast(data=df, t_ahead_series=True)

    # df = PythonEntsoeSource(
    #     api_key="9cbc6eb1-6b03-413e-b1c4-ef05ee3c8f11",
    #     start=pd.Timestamp("20230829", tz="Europe/Amsterdam"),
    #     end=pd.Timestamp("20230830", tz="Europe/Amsterdam"),
    #     country_code="NL",
    #     resolution="60T",
    # ).read_batch()
    # x = db.write_apx_market_data(df)

    # df = PythonMFFBASSource(start="2023-08-29", end="2023-08-30").read_batch()
    # x = db.write_sjv_load_profiles(df, field_columns=list(df.columns)[:-1])

    # data = [["Enercon E101", 4, 24, "onshore", "Enercon", 3040270, 3000000, 7.91, 0.76]]
    # columns = [
    #     "name",
    #     "cut_in",
    #     "cut_off",
    #     "kind",
    #     "manufacturer",
    #     "peak_capacity",
    #     "rated_power",
    #     "slope_center",
    #     "steepness",
    # ]
    # df = pd.DataFrame(data, columns=columns)
    # data = {
    #     "name": ["Test Name"],
    #     "cut_in": [3],
    #     "cut_off": [25],
    #     "kind": ["onshore"],
    #     "manufacturer": ["Test Manfacturer"],
    #     "peak_capacity": [111],
    #     "rated_power": [222],
    #     "slope_center": [7.7],
    #     "steepness": [0.7],
    # }
    # df = pd.DataFrame(data)
    # x = db.write_windturbine_powercurves(df, if_exists="append")

    # data = {
    #     "id": [1234567],
    #     "pid": [317],
    #     "date_start": ["2020-12-08"],
    #     "date_end": ["2021-03-08"],
    #     "created": ["2023-10-05T10:14:19"],
    #     "coef_name": ["Test"],
    #     "coef_value": [0],
    # }
    # df = pd.DataFrame(data)
    # x = db.write_energy_splitting_coefficients(df, "append")

    ##### PREDICTION JOB METHODS
    # x = db.get_prediction_jobs_solar()
    # x = db.get_prediction_jobs_wind()
    # x = db.get_prediction_jobs()
    # x = db.get_prediction_job(317)
    # x = db.get_pids_for_api_key("uuid-Location_C")
    # x = db.get_pids_for_api_keys()
    # x = db.get_ean_for_pid(313)
    # x = db.get_eans_for_pids(
    #     [313, 317]
    # )

    ##### WEATHER METHODS
    # x = db.get_weather_forecast_locations()
    x = db.get_weather_data(
        location="Deelen",
        weatherparams=["pressure", "temp"],
        datetime_start=datetime(2023, 8, 29),
        datetime_end=datetime(2023, 8, 30),
        source="harm_arome",
    )
    # x = db.get_datetime_last_stored_knmi_weatherdata()

    ##### PREDCITOR METHODS
    # x = db.get_predictors(
    #     datetime_start=datetime(2023, 8, 29),
    #     datetime_end=datetime(2023, 8, 30),
    #     location=(pj["lat"], pj["lon"]),
    #     forecast_resolution="15min",
    #     predictor_groups=["market_data", "weather_data", "load_profiles"],
    # )
    # x = db.get_electricity_price(datetime(2023, 9, 30), datetime(2023, 10, 2))
    # x = db.get_load_profiles(
    #     datetime_start=datetime(2023, 8, 29),
    #     datetime_end=datetime(2023, 8, 30),
    #     forecast_resolution="15min",
    # )

    ##### HISTORY CDB DATA SERVICE
    # x = db.get_load_sid(
    #     sid=["Location_A_System_1", "Location_A_System_10", "Location_C_System_2"],
    #     datetime_start=datetime(2023, 8, 29),
    #     datetime_end=datetime(2023, 8, 30),
    #     forecast_resolution="15T",
    #     aggregated=False,
    #     average_output=True,
    #     include_n_entries_column=False,
    # )
    # x = db.get_load_pid(
    #     313,
    #     datetime(2023, 8, 29),
    #     datetime(2023, 8, 30),
    #     aggregated=True,
    #     ignore_factor=True,
    # )

    ##### SPLITTING METHODS
    # x = db.get_wind_ref("Deelen", datetime(2023, 8, 29), datetime(2023, 8, 30))
    # x = db.get_energy_split_coefs({"id": "317"})
    # x = db.get_input_energy_splitting(
    #     pj=pj, datetime_start=datetime(2024, 8, 29), datetime_end=datetime(2024, 8, 30)
    # )

    ##### PREDICTIONS METHODS
    # x = db.get_predicted_load(
    #     {"id": "459", "resolution_minutes": 3},
    #     datetime(2023, 10, 24),
    #     datetime(2023, 10, 30),
    # )
    # x = db.get_predicted_load_tahead(
    #     start_time=datetime(2023, 8, 29),
    #     end_time=datetime(2023, 8, 30),
    #     pj={"id": "317", "resolution_minutes": 3},
    #     t_ahead=["8h", "24h"],
    # )
    # x = db.get_prediction_including_components(
    #     start_time=datetime(2023, 10, 24),
    #     end_time=datetime(2023, 10, 30),
    #     pj={"id": "459", "quantiles": [0.10, 0.05], "resolution_minutes": 3},
    # )
    # x = db.get_forecast_quality(
    #     start_time=datetime(2023, 10, 24),
    #     end_time=datetime(2023, 10, 30),
    #     pj={"id": "459", "resolution_minutes": 3},
    # )

    ##### MODEL INPUT METHODS
    # x = db.get_model_input(
    #     pid=pj["id"],
    #     location=[pj["lat"], pj["lon"]],
    #     datetime_start=datetime(2023, 8, 29),
    #     datetime_end=datetime(2023, 8, 30),
    # )
    # x = db.get_wind_input(
    #     (pj["lat"], pj["lon"]),
    #     pj["hub_height"],
    #     pj["horizon_minutes"],
    #     pj["resolution_minutes"],
    #     datetime_start=datetime(2023, 8, 1),
    # )
    # x = db.get_power_curve("Enercon E101")
    # x = db.get_solar_input(
    #     location=(pj["lat"], pj["lon"]),
    #     forecast_horizon=pj["horizon_minutes"],
    #     forecast_resolution=pj["resolution_minutes"],
    #     datetime_start=datetime(2023, 9, 4),
    # )  # [PARSE_SYNTAX_ERROR] Syntax error at or near ''distance''.(line 2, pos 240)

    ##### SYSTEMS METHODS
    # x = db.get_systems_near_location(
    #     (51.813, 5.837)
    # )  # [PARSE_SYNTAX_ERROR] Syntax error at or near ''distance''.(line 2, pos 240)
    # x = db.get_systems_by_pid(313)
    # x = db.get_pv_systems_with_incorrect_location()
    # x = db.get_random_pv_systems(10)
    # x = db.get_api_key_for_system("Location_A_System_1")
    # x = db.get_api_keys_for_systems(["Location_A_System_1"])

    print(x)
