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

import json
from web3 import Web3
from ..._pipeline_utils.models import Libraries, SystemType
from ...destinations.interfaces import DestinationInterface


class EVMContractDestination(DestinationInterface):
    """
    The EVM Contract Destination is used to write to a smart contract blockchain.

    Examples
    --------
    ```python
    from rtdip_sdk.pipelines.destinations import EVMContractDestination

    evm_contract_destination = EVMContractDestination(
        url="https://polygon-mumbai.g.alchemy.com/v2/⟨API_KEY⟩",
        account="{ACCOUNT-ADDRESS}",
        private_key="{PRIVATE-KEY}",
        abi="{SMART-CONTRACT'S-ABI}",
        contract="{SMART-CONTRACT-ADDRESS}",
        function_name="{SMART-CONTRACT-FUNCTION}",
        function_params=({PARAMETER_1}, {PARAMETER_2}, {PARAMETER_3}),
        transaction={'gas': {GAS}, 'gasPrice': {GAS-PRICE}},
    )

    evm_contract_destination.write_batch()
    ```

    Parameters:
        url (str): Blockchain network URL e.g. 'https://polygon-mumbai.g.alchemy.com/v2/⟨API_KEY⟩'
        account (str): Address of the sender that will be signing the transaction.
        private_key (str): Private key for your blockchain account.
        abi (json str): Smart contract's ABI.
        contract (str): Address of the smart contract.
        function_name (str): Smart contract method to call on.
        function_params (tuple): Parameters of given function.
        transaction (dict): A dictionary containing a set of instructions to interact with a smart contract deployed on the blockchain (See common parameters in Attributes table below).

    Attributes:
        data (hexadecimal str): Additional information store in the transaction.
        from (hexadecimal str): Address of sender for a transaction.
        gas (int): Amount of gas units to perform a transaction.
        gasPrice (int Wei): Price to pay for each unit of gas. Integers are specified in Wei, web3's to_wei function can be used to specify the amount in a different currency.
        nonce (int): The number of transactions sent from a given address.
        to (hexadecimal str): Address of recipient for a transaction.
        value (int Wei): Value being transferred in a transaction. Integers are specified in Wei, web3's to_wei function can be used to specify the amount in a different currency.
    """

    url: str
    account: str
    private_key: str
    abi: str
    contract: str
    function_name: str
    function_params: tuple
    transaction: dict

    def __init__(
        self,
        url: str,
        account: str,
        private_key: str,
        abi: str,
        contract: str = None,
        function_name: str = None,
        function_params: tuple = None,
        transaction: dict = None,
    ) -> None:
        self.url = url
        self.account = account
        self.private_key = private_key
        self.abi = json.loads(abi)
        self.contract = contract
        self.function_name = function_name
        self.function_params = function_params
        self.transaction = transaction
        self.web3 = Web3(Web3.HTTPProvider(self.url))

    @staticmethod
    def system_type():
        return SystemType.PYTHON

    @staticmethod
    def libraries():
        libraries = Libraries()
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def pre_write_validation(self) -> bool:
        return True

    def post_write_validation(self) -> bool:
        return True

    def _process_transaction(self):
        if "nonce" not in self.transaction.keys():
            nonce = self.web3.eth.get_transaction_count(self.account)
            self.transaction["nonce"] = nonce
        if "from" not in self.transaction.keys():
            self.transaction["from"] = self.account

    def write_batch(self) -> str:
        """
        Writes to a smart contract deployed in a blockchain and returns the transaction hash.

        Example:
        ```
        from web3 import Web3

        web3 = Web3(Web3.HTTPProvider("https://polygon-mumbai.g.alchemy.com/v2/<API_KEY>"))

        x = EVMContractDestination(
                            url="https://polygon-mumbai.g.alchemy.com/v2/<API_KEY>",
                            account='<ACCOUNT>',
                            private_key='<PRIVATE_KEY>',
                            contract='<CONTRACT>',
                            function_name='transferFrom',
                            function_params=('<FROM_ACCOUNT>', '<TO_ACCOUNT>', 0),
                            abi = 'ABI',
                            transaction={
                                'gas': 100000,
                                'gasPrice': 1000000000 # or web3.to_wei('1', 'gwei')
                                },
                            )

        print(x.write_batch())
        ```
        """
        contract = self.web3.eth.contract(address=self.contract, abi=self.abi)

        self._process_transaction()
        tx = contract.functions[self.function_name](
            *self.function_params
        ).build_transaction(self.transaction)

        signed_tx = self.web3.eth.account.sign_transaction(tx, self.private_key)
        tx_hash = self.web3.eth.send_raw_transaction(signed_tx.rawTransaction)
        self.web3.eth.wait_for_transaction_receipt(tx_hash)

        return str(self.web3.to_hex(tx_hash))

    def write_stream(self):
        """
        Raises:
            NotImplementedError: Write stream is not supported.
        """
        raise NotImplementedError("EVMContractDestination only supports batch writes.")
