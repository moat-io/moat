import requests

client_id = "moat-api-client"
client_secret = "f0YyuWKVknvonyOzOfGwKOws6802BwMp"  # local mocked secret only

openid_config_response = requests.get(
    "http://localhost:8080/realms/moat/.well-known/openid-configuration"
)
openid_config = openid_config_response.json()

token_endpoint = openid_config["token_endpoint"]

data = {
    "grant_type": "client_credentials",
    "client_id": client_id,
    "client_secret": client_secret,
    "scope": "scim-read scim-write",
}

token_response = requests.post(token_endpoint, data=data)
token_response.raise_for_status()

access_token = token_response.json()["access_token"]
print(f"Token: {access_token}")

print()
print("Testing with valid token:")
api_response = requests.get(
    "http://localhost:8000/api/v1/healthcheck",
    headers={"Authorization": f"Bearer {access_token}"},
)

print(api_response.json())

print()
print("Testing with INVALID token:")
api_response = requests.get(
    "http://localhost:8000/api/v1/healthcheck",
    headers={"Authorization": f"Bearer {access_token}abcd"},
)
print(api_response.json())
