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

import sys
sys.path.insert(0, '.')
import pytest
from src.sdk.python.rtdip_sdk.pipelines.destinations.blockchain.evm import EVMContractDestination
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries
from pytest_mock import MockerFixture

WEB3_CONTRACT = "web3.eth.Eth.contract"
WEB3_GET_TRANSACTION_COUNT = "web3.eth.Eth.get_transaction_count"
WEB3_ACCOUNT = "web3.eth.Eth.account"
WEB3_SEND_RAW_TRANSACTION = "web3.eth.Eth.send_raw_transaction"
WEB3_WAIT_FOR_TRASACTION_RECEIPT = "web3.eth.Eth.wait_for_transaction_receipt"
WEB3_TO_HEX = "web3.Web3.to_hex"
URL = "https://mockedurl"
ACCOUNT = "mocked_account"
PRIVATE_KEY = "mocked_privatekey"
ABI = "[{\"inputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"constructor\"}]"
CONTRACT = "mocked_contract"
FUNCTION_NAME = "mocked_function_name"
FUNCTION_PARAMS = ('mocked_params1', 'mocked_params2')
TRANSACTION = {'gas': 200000, 'gasPrice': 1000000000}
# WEB3 = Web3(Web3.HTTPProvider(self.url))

# class MockedContract():
#     def functions(self):
#         return MockedFunctions()

class MockedWeb3():
    def functions(self):
        return MockedFunctions()
    # def account():
    #     return MockedAccount()
    
class MockedFunctions():
    def build_transactions():
        return None

class MockedAccount():
    def sign_transaction():
        return None

def test_polygon_write_setup():
    polygon_destination = EVMContractDestination("url", "account", "private_key", '[{"abi":[]}]')
    assert polygon_destination.system_type().value == 1
    assert polygon_destination.libraries() == Libraries(maven_libraries=[], pypi_libraries=[], pythonwheel_libraries=[])
    assert isinstance(polygon_destination.settings(), dict)
    assert polygon_destination.pre_write_validation()
    assert polygon_destination.post_write_validation()

def test_polygon_write_batch(mocker: MockerFixture):
    mocker.patch(WEB3_CONTRACT, return_value = MockedWeb3()) 
    mocker.patch(WEB3_ACCOUNT, return_value = MockedAccount())
    mocker.patch(WEB3_GET_TRANSACTION_COUNT, return_value = 1)
    mocker.patch(WEB3_SEND_RAW_TRANSACTION, return_value = bytes)
    mocker.patch(WEB3_WAIT_FOR_TRASACTION_RECEIPT, return_value = None)
    mocker.patch(WEB3_TO_HEX, return_value = hex)
    mocker.patch.object(MockedWeb3, "functions", return_value = MockedFunctions())
    mocker.spy(MockedFunctions, "build_transactions") 
    mocker.spy(MockedAccount, "sign_transaction")
   
    polygon_destination = EVMContractDestination(URL, ACCOUNT, PRIVATE_KEY, ABI, CONTRACT, FUNCTION_NAME, FUNCTION_PARAMS, TRANSACTION)

    actual = polygon_destination.write_batch()

    assert isinstance(actual, str)

def test_polygon_write_batch_fails(mocker: MockerFixture):
    mocker.patch(WEB3_CONTRACT, side_effect=Exception) 
    mocker.patch(WEB3_ACCOUNT, return_value = MockedAccount())
    mocker.patch(WEB3_GET_TRANSACTION_COUNT, side_effect=Exception)
    mocker.patch(WEB3_SEND_RAW_TRANSACTION, side_effect=Exception)
    mocker.patch(WEB3_WAIT_FOR_TRASACTION_RECEIPT, side_effect=Exception)
    mocker.patch(WEB3_TO_HEX, return_value = hex)
    mocker.patch.object(MockedWeb3, "functions", side_effect=Exception)
    mocker.spy(MockedFunctions, "build_transactions") 
    mocker.spy(MockedAccount, "sign_transaction")
   
    polygon_destination = EVMContractDestination(URL, ACCOUNT, PRIVATE_KEY, ABI, CONTRACT, FUNCTION_NAME, FUNCTION_PARAMS, TRANSACTION)

    with pytest.raises(Exception):
        polygon_destination.write_batch()

def test_polygon_write_stream():
    with pytest.raises(NotImplementedError) as excinfo:
        tx = {'gas': 200000, 'gasPrice': '2 gwei'}
        polygon_destination = EVMContractDestination("url", "account", "private_key", '[{"abi":[]}]', "contract", "function", "params", tx)
        polygon_destination.write_stream()

    assert str(excinfo.value) == 'EVMContractDestination only supports batch writes.' 
