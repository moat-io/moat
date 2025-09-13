# Authentication & Authorization
Moat APIs support three authentication methods: `none`, `api-key` or `oauth2`.
Each API can support a separate authentication method

## Configuration
Configuration values take the form `api.<resource-name>.<config-parameter>`

```yaml
# examples of auth methods per API
api.healthcheck.auth_method: none # disables authentication on a resource
api.opa.auth_method: api-key
api.resources.auth_method: oauth2
```

## API Key Authentication
This method uses a pre-shared API key for authentication.
Each request is authenticated if the supplied `Bearer` token matches that stored in the config

### Configuration
```yaml
api.bundle.api-key: 6c0cbf5029aed0af395ac4b864c6b095
```

### Usage
When making API requests, include the API key in the HTTP header:

```bash
curl -X GET https://your-moat-instance/api/endpoint \
  -H "Authorization: Bearer 6c0cbf5029aed0af395ac4b864c6b095"
```

## OAuth2 Authentication
For more robust authentication, Moat supports OAuth2 authentication using the client credentials flow.
In this scenario, moat is acting as a `resource server` with an external `authorisation server` (e.g Keycloak, Okta)

### Configuration
```yaml
# OAuth2 authentication configuration
api.resources.auth_method: oauth2
api.resources.oauth2_issuer: https://<issuer-domain>/oauth2/<auth-server-id>
api.resources.oauth2_audience: <audience>
api.resources.oauth2_algorithms: RS256
api.resources.oauth2_jwks_uri: http://<issuer-domain>/.../certs
api.resources.oauth2_read_scope: resource_read
api.resources.oauth2_write_scope: resource_write
```

### Scopes
For all REST apis, the client requires the `api.<resource>.oauth2_read_scope` to allow `GET` requests, and 
`api.<resource>.oauth2_write_scope` for `POST, PUT, DELETE`.

Scope values are up to the implementation, provided the value of `api.<resource>.oauth2_read_scope` is on the access token,
then authorisation will be granted.

### Client Credentials Flow
The client credentials flow is designed for server-to-server authentication where a client application requests an access token using its client credentials.

#### Steps:
1. The client application authenticates with the OAuth2 provider using its client ID and client secret.
2. The client requests an access token from the authorization server's token endpoint.
3. The authorization server authenticates the client and issues an access token.
4. The client uses the access token to authenticate requests to the Moat API.
5. The moat API validates the token against the authorization server 
