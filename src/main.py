from flask import Flask, request, jsonify
import uuid
from datetime import datetime, timezone

app = Flask(__name__)

@app.route("/", methods=["GET"])
def health_check():
    return jsonify({
        "status": "ok",
        "message": "Cloud Run service is running"
    }), 200


@app.route("/ingest", methods=["POST"])
def ingest():
    # Try to parse JSON
    payload = request.get_json(silent=True)

    if payload is None:
        return jsonify({
            "error": "Invalid or missing JSON body"
        }), 400

    # Example: get optional parameters
    target = request.args.get("target") or payload.get("target", "default")
    source = request.args.get("source", "unknown")

    # Add metadata (this is what you'd later send to BigQuery)
    enriched = {
        "request_id": str(uuid.uuid4()),
        "received_at": datetime.now(timezone.utc).isoformat(),
        "target": target,
        "source": source,
        "payload": payload
    }

    # For now, just log it (Cloud Run logs)
    print("Received webhook:")
    print(enriched)

    # Return response
    return jsonify({
        "status": "received",
        "target": target,
        "request_id": enriched["request_id"]
    }), 200


if __name__ == "__main__":
    # Cloud Run requires listening on PORT env var
    import os
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)