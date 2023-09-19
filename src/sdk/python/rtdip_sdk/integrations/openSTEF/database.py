# Copyright 2022 RTDIP
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

from openstef_dbc import Singleton
from openstef_dbc.services.ems import Ems
from openstef_dbc.services.model_input import ModelInput
from openstef_dbc.services.prediction_job import PredictionJobRetriever
from openstef_dbc.services.predictions import Predictions
from openstef_dbc.services.predictor import Predictor
from openstef_dbc.services.splitting import Splitting
from openstef_dbc.services.systems import Systems
from openstef_dbc.services.weather import Weather
from openstef_dbc.services.write import Write
from .interfaces import _DataInterface


class DataBase(metaclass=Singleton):
    """Provides a high-level interface to various data sources.

    All user/client code should use this class to get or write data. Under the hood
    this class uses various services to interfact with its datasource.
    """

    _instance = None

    # services
    _write = Write()
    _prediction_job = PredictionJobRetriever()
    _weather = Weather()
    _historic_cdb_data_service = Ems()
    _predictor = Predictor()
    _splitting = Splitting()
    _predictions = Predictions()
    _model_input = ModelInput()
    _systems = Systems()

    # write methods
    write_weather_data = _write.write_weather_data
    write_realised = _write.write_realised
    write_realised_pvdata = _write.write_realised_pvdata
    write_kpi = _write.write_kpi
    write_forecast = _write.write_forecast
    write_apx_market_data = _write.write_apx_market_data
    write_sjv_load_profiles = _write.write_sjv_load_profiles
    write_windturbine_powercurves = _write.write_windturbine_powercurves
    write_energy_splitting_coefficients = _write.write_energy_splitting_coefficients

    # prediction job methods
    get_prediction_jobs_solar = _prediction_job.get_prediction_jobs_solar
    get_prediction_jobs_wind = _prediction_job.get_prediction_jobs_wind
    get_prediction_jobs = _prediction_job.get_prediction_jobs
    get_prediction_job = _prediction_job.get_prediction_job
    get_pids_for_api_key = _prediction_job.get_pids_for_api_key
    get_pids_for_api_keys = _prediction_job.get_pids_for_api_keys
    get_ean_for_pid = _prediction_job.get_ean_for_pid
    get_eans_for_pids = _prediction_job.get_eans_for_pids

    # weather methods
    get_weather_forecast_locations = _weather.get_weather_forecast_locations
    get_weather_data = _weather.get_weather_data
    get_datetime_last_stored_knmi_weatherdata = (
        _weather.get_datetime_last_stored_knmi_weatherdata
    )
    # predictor methods
    get_predictors = _predictor.get_predictors
    get_electricity_price = _predictor.get_electricity_price
    get_load_profiles = _predictor.get_load_profiles
    # historic cdb data service
    get_load_sid = _historic_cdb_data_service.get_load_sid
    get_load_pid = _historic_cdb_data_service.get_load_pid

    # splitting methods
    get_wind_ref = _splitting.get_wind_ref
    get_energy_split_coefs = _splitting.get_energy_split_coefs
    get_input_energy_splitting = _splitting.get_input_energy_splitting
    # predictions methods
    get_predicted_load = _predictions.get_predicted_load
    get_predicted_load_tahead = _predictions.get_predicted_load_tahead
    get_prediction_including_components = (
        _predictions.get_prediction_including_components
    )
    get_forecast_quality = _predictions.get_forecast_quality
    # model input methods
    get_model_input = _model_input.get_model_input
    get_wind_input = _model_input.get_wind_input
    get_power_curve = _model_input.get_power_curve
    get_solar_input = _model_input.get_solar_input
    # systems methods
    get_systems_near_location = _systems.get_systems_near_location
    get_systems_by_pid = _systems.get_systems_by_pid
    get_pv_systems_with_incorrect_location = (
        _systems.get_pv_systems_with_incorrect_location
    )
    get_random_pv_systems = _systems.get_random_pv_systems
    get_api_key_for_system = _systems.get_api_key_for_system
    get_api_keys_for_systems = _systems.get_api_keys_for_systems

    def __init__(self, config):
        """Construct the DataBase singleton.

        Initialize the datainterface and api. WARNING: this is a singleton class when
        calling multiple times with a config argument no new configuration will be
        applied.

        Args:
            config: Configuration object. with the following attributes:
                api_username (str): API username.
                api_password (str): API password.
                api_admin_username (str): API admin username.
                api_admin_password (str): API admin password.
                api_url (str): API url.
                db_host (str): Databricks hostname.
                db_token (str): Databricks token.
                db_port (int): Databricks port.
                db_catalog (str): Databricks catalog.
                db_schema (str): Databricks schema.
                db_http_path (str): SQL warehouse http path.
                proxies Union[dict[str, str], None]: Proxies.

        """

        self._datainterface = _DataInterface(config)
        # Ktp api
        self.ktp_api = self._datainterface.ktp_api

        DataBase._instance = self
