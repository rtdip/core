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

WEB3 = "web3.eth"
URL = "https://mockedurl"
ACCOUNT = "mocked_account"
PRIVATE_KEY = "mocked_privatekey"
ABI = "[{\"inputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"constructor\"}]"
CONTRACT = "mocked_contract"
FUNCTION_NAME = "mocked_function_name"
FUNCTION_PARAMS = ('mocked_params1', 'mocked_params2')
TRANSACTION = {'gas': 200000, 'gasPrice': 1000000000}
# WEB3 = Web3(Web3.HTTPProvider(self.url))

class MockedContract():
    def functions(self):
        return MockedFunctions()
    
class MockedFunctions():
    def build_transactions():
        return None
    
class MockedWeb3():
    def contracts():
        return None
    def account():
        return MockedAccount()
    def send_raw_transaction():
        return bytes
    def wait_for_transaction_receipt():
        return None
    def to_hex():
        return hex

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
    mocker.patch("web3.eth.Eth.contract", return_value = MockedContract())
    mocker.patch("web3.eth.Eth.get_transaction_count", return_value = 1)
    mocker.patch.object(MockedContract, "functions", return_value = MockedFunctions())
    mocker.spy(MockedFunctions, "build_transactions")
    mocker.patch("web3.eth.Eth.account", return_value = MockedAccount())
    mocker.patch.object(MockedWeb3, "account", return_value = MockedAccount())
    mocker.spy(MockedAccount, "sign_transaction")
    mocker.patch("web3.eth.Eth.send_raw_transaction", return_value = bytes)
    mocker.patch("web3.eth.Eth.wait_for_transaction_receipt", return_value = None)
    mocker.patch("web3.Web3.to_hex", return_value = hex)

    # mocked_contract = mocker.spy(MockedWeb3, "contract")
    # mocked_account = mocker.spy(MockedWeb3, "account")
    # mocked_sign_transaction = mocker.spy(MockedAccount, "sign_transaction")
    # mocked_send_raw_transaction = mocker.spy(MockedWeb3, "send_raw_transaction")
    # mocked_wait_for_transaction_receipt = mocker.spy(MockedWeb3, "wait_for_transaction_receipt")

    polygon_destination = EVMContractDestination(URL, ACCOUNT, PRIVATE_KEY, ABI, CONTRACT, FUNCTION_NAME, FUNCTION_PARAMS, TRANSACTION)

    
    actual = polygon_destination.write_batch()
    # 4670036112

    # mocked_contract.assert_called_once()
    # mocked_account.assert_called_once()
    # mocked_sign_transaction.assert_called_once()
    # mocked_send_raw_transaction.assert_called_once()
    # mocked_wait_for_transaction_receipt.assert_called_once()
    assert isinstance(actual, str)


def test_polygon_write_stream():
    with pytest.raises(NotImplementedError) as excinfo:
        tx = {'gas': 200000, 'gasPrice': '2 gwei'}
        polygon_destination = EVMContractDestination("url", "account", "private_key", '[{"abi":[]}]', "contract", "function", "params", tx)
        polygon_destination.write_stream()

    assert str(excinfo.value) == 'EVMContractDestination only supports batch writes.' 
