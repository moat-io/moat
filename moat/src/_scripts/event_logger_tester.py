import requests
import time

for i in range(100):
    response = requests.get(
        "http://localhost:8000/api/v1/healthcheck",
    )
    print(response.json())
