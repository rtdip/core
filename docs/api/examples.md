# Examples

Below are examples of how to execute APIs using various authentication options and API methods.

## End User Authentication 

### Python

A python example of obtaining a token as a user can be found below using the `azure-identity` python package to authenticate with Azure AD.

!!! note "POST Requests"
    The POST request can be used to pass many tags to the API. This is the preferred method when passing large volumes of tags to the API.

=== "GET Request"
    ```python
    from azure.identity import DefaultAzureCredential
    import requests

    authentication = DefaultAzureCredential()
    access_token = authentication.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token

    params = {
    "business_unit": "Business Unit",
    "region": "Region",
    "asset": "Asset Name",
    "data_security_level": "Security Level",
    "data_type": "float",
    "tag_name": "TAG1",
    "tag_name": "TAG2",
    "start_date": "2022-01-01",
    "end_date": "2022-01-01",
    "include_bad_data": True
    }

    url = "https://example.com/api/v1/events/raw"

    payload={}
    headers = {
    'Authorization': 'Bearer {}'.format(access_token)
    }

    response = requests.request("GET", url, headers=headers, params=params, data=payload)

    print(response.json())
    ```

=== "POST Request"
    ```python
    from azure.identity import DefaultAzureCredential
    import requests

    authentication = DefaultAzureCredential()
    access_token = authentication.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token

    params = {
    "business_unit": "Business Unit",
    "region": "Region",
    "asset": "Asset Name",
    "data_security_level": "Security Level",
    "data_type": "float",
    "start_date": "2022-01-01T15:00:00",
    "end_date": "2022-01-01T16:00:00",
    "include_bad_data": True    
    }

    url = "https://example.com/api/v1/events/raw"

    payload={"tag_name": ["TAG1", "TAG2"]}

    headers = {
    "Authorization": "Bearer {}".format(access_token),
    }

    # Requests automatically sets the Content-Type to application/json when the request body is passed via the json parameter
    response = requests.request("POST", url, headers=headers, params=params, json=payload)

    print(response.json())
    ```

## Service Principal Authentication 

### GET Request

Authentication using Service Principals is similar to end user authentication. An example, using Python is provided below where the `azure-identity` package is not used, instead a direct REST API call is made to retrieve the token.

=== "cURL"
    ```curl
    curl --location --request POST 'https://login.microsoftonline.com/{tenant id}/oauth2/v2.0/token' \
    --form 'grant_type="client_credentials"' \
    --form 'client_id="<<client id>>"' \
    --form 'client_secret="<<client secret>>"' \
    --form 'scope="2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"'
    ```

=== "Python"
    ```python
    import requests

    url = "https://login.microsoftonline.com/{tenant id}/oauth2/v2.0/token"

    payload={'grant_type': 'client_credentials',
    'client_id': '<<client id>>',
    'client_secret': '<<client secret>>',
    'scope': '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default'}
    files=[]
    headers = {}

    response = requests.request("POST", url, headers=headers, data=payload, files=files)

    access_token  = response.json()["access_token"]

    params = {
        "business_unit": "Business Unit",
        "region": "Region",
        "asset": "Asset Name",
        "data_security_level": "Security Level",
        "data_type": "float",
        "tag_name": "TAG1",
        "tag_name": "TAG2",
        "start_date": "2022-01-01",
        "end_date": "2022-01-01",
        "include_bad_data": True
    }

    url = "https://example.com/api/v1/events/raw"

    payload={}
    headers = {
    'Authorization': 'Bearer {}'.format(access_token)
    }

    response = requests.request("GET", url, headers=headers, params=params, data=payload)

    print(response.text)
    ```