"""
A simple Flask app that provides a test API for event logging.
It has one POST endpoint at /event that prints the received content to the console.
"""

from flask import Flask, request, jsonify

app = Flask(__name__)


@app.route("/event", methods=["POST"])
def log_event():
    """
    Endpoint to receive event data and print it to the console.
    """
    content = request.json
    print("Received event:")
    print(content)
    return jsonify({"status": "success", "message": "Event received"})


app.run(host="0.0.0.0", port=9000, debug=True)
