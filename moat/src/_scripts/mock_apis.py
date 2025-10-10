"""
A simple Flask app that provides a test API for event logging.
It has one POST endpoint at /event that prints the received content to the console.
"""

from flask import Flask, request, jsonify

app = Flask(__name__)


"""
Event logger API mock for Datadog, Newrelic etc
"""


@app.route("/event", methods=["POST"])
def log_event():
    """
    Endpoint to receive event data and print it to the console.
    """
    content = request.json
    print("Received event:")
    print(content)
    return jsonify({"status": "success", "message": "Event received"})


@app.route("/oauth2/token", methods=["POST"])
def oauth2_token():
    return jsonify({"access_token": "test-token"})


"""
Users endpoint to test http ingestion connector
"""


@app.route("/users")
def get_users():
    return jsonify(
        [
            {
                "attributes": {
                    "loginID": "onyangokariuiki",
                    "displayName": "Onyango Kariuiki",
                    "familyName": "kariuiki",
                    "givenName": "onyango",
                    "active": "true",
                    "externalId": None,
                    "userName": "onyangokariuiki@aol.com.io",
                    "email": "onyangokariuiki@aol.com.io",
                    "group": ["ReadAllNonSensitive Sales", "ADGROUP::ABCD_All_Users"],
                    "urn:ietf:params:scim:enterpriseExtended.username": "onyangokariuiki",
                },
            },
            {
                "attributes": {
                    "loginID": "tomtakahara",
                    "displayName": "Tom Takahara",
                    "familyName": "takahara",
                    "givenName": "tom",
                    "active": "true",
                    "externalId": None,
                    "userName": "tomtakahara@aol.com.io",
                    "email": "tomtakahara@aol.com.io",
                    "group": [
                        "ReadAllNonSensitive Sales",
                        "ADGROUP::ABCD_All_Users",
                        "ReadAllNonSensitive Marketing",
                        "ReadAllNonSensitive HT",
                        "ADGROUP::ABCD_All_Admins",
                        "ReadSensitive IT",
                        "DP Customer Information",
                    ],
                    "urn:ietf:params:scim:enterpriseExtended.username": "tomtakahara",
                },
            },
        ]
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9000, debug=True)
