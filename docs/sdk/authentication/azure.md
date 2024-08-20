# Azure Active Directory

The RTDIP SDK includes several Azure AD authentication methods to cater to the preference of the user:

* [Default Authentication](../code-reference/authentication/azure.md) - For authenticating users with Azure AD using the [azure-identity](https://docs.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential?view=azure-python) package. Note the order that Default Authentication uses to sign in a user and how it does it in this [documentation](https://azuresdkdocs.blob.core.windows.net/$web/python/azure-identity/1.10.0b1/index.html). From experience, the Visual Studio Code login is the easiest to setup, but the azure cli option is the most reliable option. This [page](https://github.com/Azure/azure-sdk-for-python/blob/main/sdk/identity/azure-identity/TROUBLESHOOTING.md#troubleshooting-default-azure-credential-authentication-issues) is useful for troubleshooting issues with this option to authenticate.

!!! note "Visual Studio Code"
    As per the guidance in the documentation - **To authenticate in Visual Studio Code, ensure version 0.9.11 or earlier of the Azure Account extension is installed. To track progress toward supporting newer extension versions, see [this GitHub issue](https://github.com/Azure/azure-sdk-for-net/issues/27263). Once installed, open the Command Palette and run the Azure: Sign In command**.


* [Certificate Authentication](../code-reference/authentication/azure.md) - Service Principal authentication using a certificate

* [Client Secret Authentication](../code-reference/authentication/azure.md) - Service Principal authentication using a client id and client secret

## Authentication

<!-- --8<-- [start:azuread] -->

The following section describes authentication using [Azure Active Directory.](https://www.rtdip.io/sdk/code-reference/authentication/azure/).

!!! note "Note"
       If you are using the SDK directly in Databricks please note that DefaultAuth will not work.

1\. Import **rtdip-sdk** authentication methods with the following:

    from rtdip_sdk.authentication import azure as auth

2\. Use any of the following authentication methods. Replace **tenant_id** , **client_id**, **certificate_path** or **client_secret** with your own details.

=== "Default Authentication"
        credential = auth.DefaultAuth().authenticate()
    
=== "Certificate Authentication"
        credential = auth.CertificateAuth(tenant_id, client_id, certificate_path).authenticate()

=== "Client Secret Authentication"
        credential = auth.ClientSecretAuth(tenant_id, client_id, client_secret).authenticate()

3\. The methods above will return back a Client Object. The following example will show you how to retrieve the access_token from a credential object. The access token will be used in later steps to connect to RTDIP via the three options (Databricks SQL Connect, PYODBC SQL Connect, TURBODBC SQL Connect).
    
### Tokens

Once authenticated, it is possible to retrieve tokens for specific Azure Resources by providing scopes when retrieving tokens. Please see below for examples of how to retrieve tokens for Azure resources regularly used in RTDIP.

=== "Databricks"
        access_token = credential.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token

<!-- --8<-- [end:azuread] -->

!!! note "Note"
    RTDIP are continuously adding more to this list so check back regularly!

