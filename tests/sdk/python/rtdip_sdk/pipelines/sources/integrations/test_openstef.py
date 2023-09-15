import sys

sys.path.insert(0, ".")

from pydantic import BaseSettings
from typing import Union
from pytest_mock import MockerFixture
from datetime import datetime
from src.sdk.python.rtdip_sdk.integrations.openSTEF.interfaces import _DataInterface
from src.sdk.python.rtdip_sdk.integrations.openSTEF.database import DataBase

import pandas as pd

class ConfigSettigns(BaseSettings):
    api_username: str = "None"
    api_password: str = "None"
    api_admin_username: str = "None"
    api_admin_password: str = "None"
    api_url: str = "None"
    db_host: str = ""
    db_token: str = ""
    db_port: int = 443
    db_http_path: str = ""
    db_catalog: str = ""
    db_schema: str = ""
    proxies: Union[dict[str, str], None] = None


config = ConfigSettigns()


def test_methods(mocker: MockerFixture):
    db = DataBase(config)

    # mocker.patch("openstef_dbc.data_interface._DataInterface", new=_DataInterface)

    # # write methods
    # x = db.write_weather_data # influx
    # x = db.write_realised # influx
    # x = db.write_realised_pvdata # influx
    # x = db.write_kpi # influx
    # x = db.write_forecast # influx
    # x = db.write_apx_market_data # influx
    # x = db.write_sjv_load_profiles # influx

    # data = [['Enercon E101', 4, 24, 'onshore', 'Enercon', 3040270, 3000000, 7.91, 0.76]]
    # columns = ['name', 'cut_in', 'cut_off', 'kind', 'manufacturer', 'peak_capacity', 'rated_power', 'slope_center', 'steepness']
    # df = pd.DataFrame(data, columns=columns)
    # x = db.write_windturbine_powercurves(df)
    # x = db.write_energy_splitting_coefficients()

    # # prediction job methods
    # x = db.get_prediction_jobs_solar() # missing table 'solarspecs' -> GROUP BY
    # x = db.get_prediction_jobs_wind() # missing table 'windpsecs' -> GROUP BY
    # x = db.get_prediction_jobs() # GROUP BY error
    # x = db.get_prediction_job(1) # GROUP BY error
    # x = db.get_pids_for_api_key('uuid-Location_C')
    # x = db.get_pids_for_api_keys()
    # x = db.get_ean_for_pid(313)
    # x = db.get_eans_for_pids([313, 317]) # AttributeError: 'dict' object has no attribute 'append'

    # # weather methods
    # x = db.get_weather_forecast_locations()
    # x = db.get_weather_data(location='Deelen', 
    #                         weatherparams=["radiation", "temp"], 
    #                         datetime_start=datetime(2023, 8, 29), 
    #                         datetime_end=datetime(2023, 8, 30),
    #                         source="harm_arome")
    # x = db.get_datetime_last_stored_knmi_weatherdata()

    # # predictor methods
    # x = db.get_predictors(
    #         datetime_start=datetime(2023, 8, 29),
    #         datetime_end=datetime(2023, 8, 30), 
    #         location='Volkel',
    #         forecast_resolution='15T',
    #         predictor_groups=['weather_data'] # market_data, weather_data, load_profiles
    #     ) # missing table NameToLatLon
    # x = db.get_electricity_price(datetime(2023,1,1), datetime(2023,1,2)) # using influx
    # x = db.get_load_profiles() # using influx

    # historic cdb data service
    # x = db.get_load_sid(
    #     sid=['Location_A_System_1', 'Location_A_System_10', 'Location_C_System_2'],
    #     datetime_start=datetime(2023, 8, 29),
    #     datetime_end=datetime(2023, 8, 30), 
    #     forecast_resolution='15T',
    #     aggregated=True
    # )
    # x = db.get_load_pid(313, datetime(2023,1,1), datetime(2023,1,2))

    # # splitting methods
    # x = db.get_wind_ref('Deelen', datetime(2023,1,1), datetime(2023,1,2))
    x = db.get_energy_split_coefs({"id": "317"}) 
    # x = db.get_input_energy_splitting({"id": "317"}) # influx

    # # predictions methods
    # x = db.get_predicted_load({"id": "317", "resolution_minutes": 3})
    # x = db.get_predicted_load_tahead(
    #     start_time=datetime(2023, 8, 29),
    #     end_time=datetime(2023, 8, 30),
    #     pj={"id": "317", "resolution_minutes": 3},
    #     t_ahead='0'
    #     )  
    # x = db.get_prediction_including_components(
    #     start_time=datetime(2023, 8, 29),
    #     end_time=datetime(2023, 8, 30),
    #     pj={"id": "317", "quantiles": True, "resolution_minutes": 3}
    # ) 
    # x = db.get_forecast_quality(
    #     start_time=datetime(2023, 8, 29),
    #     end_time=datetime(2023, 8, 30),
    #     pj={"id": "317", "resolution_minutes": 3}
    # ) 

    # # model input methods
    # x = db.get_model_input() 
    # x = db.get_wind_input("Arnhem", 5, 5, 5) 
    # x = db.get_power_curve("Enercon E101") 
    # x = db.get_solar_input("Arnhem", 5, 5) # [PARSE_SYNTAX_ERROR] Syntax error at or near ''distance''.(line 2, pos 240)

    # # systems methods
    # x = db.get_systems_near_location("Arnhem") # [PARSE_SYNTAX_ERROR] Syntax error at or near ''distance''.(line 2, pos 240)
    # x = db.get_systems_by_pid(313)
    # x = db.get_pv_systems_with_incorrect_location() # ValueError: unsupported format character ''' (0x27) at index 71
    # x = db.get_random_pv_systems(10)
    # x = db.get_api_key_for_system('Location_A_System_1')
    # x = db.get_api_keys_for_systems(['Location_A_System_1']) 

    print(x)
