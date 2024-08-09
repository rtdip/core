<!-- --8<-- [start:authentication] -->

# Authentication 

RTDIP REST APIs require Azure Active Directory Authentication and passing the token received as an `authorization` header in the form of a Bearer token. An example of the REST API header is `Authorization: Bearer <<token>>`

## End User Authentication

If a developer or business user would like to leverage the RTDIP REST API suite, it is recommended that they use the Identity Packages provided by Azure to obtain a token.

- [REST API](https://docs.microsoft.com/en-us/azure/azure-app-configuration/rest-api-authentication-azure-ad){target=_blank}
- [.NET](https://docs.microsoft.com/en-us/dotnet/api/overview/azure/identity-readme){target=_blank}
- [Java](https://docs.microsoft.com/en-us/java/api/overview/azure/identity-readme?view=azure-java-stable){target=_blank}
- [Python](https://docs.microsoft.com/en-us/python/api/overview/azure/identity-readme?view=azure-python){target=_blank}
- [Javascript](https://docs.microsoft.com/en-us/javascript/api/overview/azure/identity-readme?view=azure-node-latest){target=_blank}

!!! note "Note"
    Note that the above packages have the ability to obtain tokens for end users and service principals and support all available authentication options. 

Ensure to install the relevant package and obtain a token.

See the [examples](https://www.rtdip.io/api/examples/) section to see various authentication methods implemented.

<!-- --8<-- [end:authentication] -->
