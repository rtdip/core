# # Copyright 2022 RTDIP
# #
# # Licensed under the Apache License, Version 2.0 (the "License");
# # you may not use this file except in compliance with the License.
# # You may obtain a copy of the License at
# #
# #      http://www.apache.org/licenses/LICENSE-2.0
# #
# # Unless required by applicable law or agreed to in writing, software
# # distributed under the License is distributed on an "AS IS" BASIS,
# # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# # See the License for the specific language governing permissions and
# # limitations under the License.

# import sys
# sys.path.insert(0, '.')
# import pytest
# from src.sdk.python.rtdip_sdk.pipelines.destinations.polygon.polygon import EVMContractDestination
# from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries
# from pytest_mock import MockerFixture

# def test_polygon_write_setup():
#     polygon_destination = EVMContractDestination("url", "account", "private_key", '[{"abi":[]}]')
#     assert polygon_destination.system_type().value == 1
#     assert polygon_destination.libraries() == Libraries(maven_libraries=[], pypi_libraries=[], pythonwheel_libraries=[])
#     assert isinstance(polygon_destination.settings(), dict)
#     assert polygon_destination.pre_write_validation()
#     assert polygon_destination.post_write_validation()

# def test_polygon_write_batch(mocker: MockerFixture):
#     mocker.patch('web3.eth.Contract', new_callable=mocker.Mock(return_value=mocker.Mock(contract=mocker.Mock(return_value=None))))
#     mocker.patch('web3.eth', new_callable=mocker.Mock(return_value=mocker.Mock(sendTransaction=mocker.Mock(return_value=None))))

#     tx = {'gas': 200000, 'gasPrice': 1000000000}
#     polygon_destination = EVMContractDestination("https://url", "account", "private_key", '[{"abi":[]}]', "contract", "function", "params", tx)

#     actual = polygon_destination.write_batch()
#     assert actual is None


# def test_polygon_write_stream():
#     with pytest.raises(NotImplementedError) as excinfo:
#         tx = {'gas': 200000, 'gasPrice': '2 gwei'}
#         polygon_destination = EVMContractDestination("url", "account", "private_key", '[{"abi":[]}]', "contract", "function", "params", tx)
#         polygon_destination.write_stream()

#     assert str(excinfo.value) == 'EVMContractDestination only supports batch writes.' 
