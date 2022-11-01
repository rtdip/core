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

from azure.identity import ClientSecretCredential, CertificateCredential, DefaultAzureCredential
import logging

class ClientSecretAuth:
    """
    Enables authentication to Azure Active Directory using a client secret that was generated for an App Registration.

    Args:
        tenant_id: The Azure Active Directory tenant (directory) Id of the service principal.
        client_id: The client (application) ID of the service principal
        client_secret: A client secret that was generated for the App Registration used to authenticate the client.
    """
    def __init__(self, tenant_id: str, client_id: str, client_secret: str) -> None:
        self.tenant_id=tenant_id
        self.client_id=client_id
        self.client_secret=client_secret

    def authenticate(self) -> ClientSecretCredential:   
        """
        Authenticates as a service principal using a client secret. 
        
        Returns:
            ClientSecretCredential: Authenticates as a service principal using a client secret.
        """     
        try:
            access_token = ClientSecretCredential(self.tenant_id, self.client_id, self.client_secret)
            return access_token
        except Exception as e:
            logging.exception('error returning client secret credential')
            raise e

class CertificateAuth:
    """
    Enables authentication to Azure Active Directory using a certificate that was generated for an App Registration.

    The certificate must have an RSA private key, because this credential signs assertions using RS256

    Args:
        tenant_id: The Azure Active Directory tenant (directory) Id of the service principal.
        client_id: The client (application) ID of the service principal
        certificate_path: Optional path to a certificate file in PEM or PKCS12 format, including the private key. If not provided, certificate_data is required.
    """
    def __init__(self, tenant_id: str, client_id: str, certificate_path:str=None) -> None:
        self.tenant_id=tenant_id
        self.client_id=client_id
        self.certificate_path=certificate_path

    def authenticate(self) -> CertificateCredential:
        """
        Authenticates as a service principal using a certificate.
        
        Returns:
            CertificateCredential: Authenticates as a service principal using a certificate.
        """
        try:   
            access_token = CertificateCredential(self.tenant_id, self.client_id, self.certificate_path)
            return access_token
        except Exception as e:
            logging.exception('error returning certificate credential')
            raise e

class DefaultAuth:
    """
    A default credential capable of handling most Azure SDK authentication scenarios.

    The identity it uses depends on the environment. When an access token is needed, it requests one using these identities in turn, stopping when one provides a token:

    1) A service principal configured by environment variables.

    2) An Azure managed identity.

    3) On Windows only: a user who has signed in with a Microsoft application, such as Visual Studio. If multiple identities are in the cache, then the value of the environment variable AZURE_USERNAME is used to select which identity to use.

    4) The user currently signed in to Visual Studio Code.

    5) The identity currently logged in to the Azure CLI.

    6) The identity currently logged in to Azure PowerShell.

    Args:
        exclude_cli_credential (Optional): Whether to exclude the Azure CLI from the credential. Defaults to False.
        exclude_environment_credential (Optional): Whether to exclude a service principal configured by environment variables from the credential. Defaults to True.
        exclude_managed_identity_credential (Optional): Whether to exclude managed identity from the credential. Defaults to True
        exclude_powershell_credential (Optional): Whether to exclude Azure PowerShell. Defaults to False.
        exclude_visual_studio_code_credential (Optional): Whether to exclude stored credential from VS Code. Defaults to False
        exclude_shared_token_cache_credential (Optional): Whether to exclude the shared token cache. Defaults to False.
        exclude_interactive_browser_credential (Optional): Whether to exclude interactive browser authentication (see InteractiveBrowserCredential). Defaults to False
        logging_enable (Optional): Turn on or off logging. Defaults to False.
    """
    def __init__(self, exclude_cli_credential=False, exclude_environment_credential=True, exclude_managed_identity_credential=True, exclude_powershell_credential=False, exclude_visual_studio_code_credential=False, exclude_shared_token_cache_credential=False, exclude_interactive_browser_credential=False, logging_enable=False) -> None:
        self.exclude_cli_credential=exclude_cli_credential
        self.exclude_environment_credential=exclude_environment_credential
        self.exclude_managed_identity_credential=exclude_managed_identity_credential
        self.exclude_powershell_credential=exclude_powershell_credential
        self.exclude_visual_studio_code_credential=exclude_visual_studio_code_credential
        self.exclude_shared_token_cache_credential=exclude_shared_token_cache_credential
        self.exclude_interactive_browser_credential=exclude_interactive_browser_credential
        self.logging_enable=logging_enable

    def authenticate(self) -> DefaultAzureCredential:
        """
        A default credential capable of handling most Azure SDK authentication scenarios.
        
        Returns:
            DefaultAzureCredential: A default credential capable of handling most Azure SDK authentication scenarios.
        """
        try:
            access_token = DefaultAzureCredential(
                exclude_cli_credential=self.exclude_cli_credential, 
                exclude_environment_credential=self.exclude_environment_credential, 
                exclude_managed_identity_credential=self.exclude_managed_identity_credential, 
                exclude_powershell_credential=self.exclude_powershell_credential, 
                exclude_visual_studio_code_credential=self.exclude_visual_studio_code_credential, 
                exclude_shared_token_cache_credential=self.exclude_shared_token_cache_credential, 
                exclude_interactive_browser_credential=self.exclude_interactive_browser_credential, 
                logging_enable=self.logging_enable)
            return access_token
        except Exception as e:
            logging.exception('error returning default azure credential')
            raise e

class Authenticator:
    """
    The class used to authenticate to different systems.
    
    Args:
        auth_class: Authentication class containing the users credentials
    """
    
    def __init__(self, auth_class) -> None:
        self.auth_class = auth_class