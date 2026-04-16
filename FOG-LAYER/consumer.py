import os
import json
from datetime import datetime, timezone
import paho.mqtt.client as mqtt

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import WriteOptions


# -----------------------
# InfluxDB Setup
# -----------------------
token = "kbURS3e9ndqfmTBulFEw8xccCecuUshg6MSELSAKCdJ2nG9o12OKQpH5hm5PGvMUd9N1GX__uWG2NjpONB7bwQ=="  # FIXED
org = os.getenv("INFLUXDB_ORG", "NEO-SOLAR")
bucket = os.getenv("INFLUXDB_BUCKET", "metrics")
url = os.getenv("INFLUXDB_URL", "http://127.0.0.1:8086")

if not token:
    raise RuntimeError("INFLUXDB_TOKEN is required")

influx = InfluxDBClient(url=url, token=token, org=org)

write_api = influx.write_api(
    write_options=WriteOptions(
        batch_size=100,
        flush_interval=5000,
        jitter_interval=2000,
        retry_interval=5000
    )
)

# -----------------------
# MQTT Config
# -----------------------
MQTT_BROKER = "127.0.0.1"
MQTT_PORT = 1883
MQTT_TOPIC = "solar/rtu/#"
DEFAULT_MEASUREMENT = "solar_rtu"
RAW_FIELD_EXCLUDE_KEYS = {"measurement", "tags", "timestamp", "node_id"}


def extract_node_id(payload, topic):
    node_id = payload.get("node_id")
    if node_id:
        return str(node_id)

    tags = payload.get("tags", {})
    if isinstance(tags, dict) and tags.get("node_id"):
        return str(tags["node_id"])

    topic_parts = topic.rstrip("/").split("/")
    if topic_parts and topic_parts[-1] not in {"solar", "rtu", "#", "+"}:
        return topic_parts[-1]

    return "unknown"


def build_point(payload):
    if not isinstance(payload, dict):
        raise ValueError("payload must be a JSON object")

    measurement = payload.get("measurement", DEFAULT_MEASUREMENT)
    point = Point(measurement)

    tags = payload.get("tags", {})
    if not isinstance(tags, dict):
        raise ValueError("payload['tags'] must be a dict")

    node_id = payload.get("node_id")
    if node_id is not None:
        tags = dict(tags)
        tags.setdefault("node_id", node_id)

    for key, value in tags.items():
        if value is not None:
            point = point.tag(str(key), str(value))

    fields = payload.get("fields")
    if fields is None:
        fields = {
            key: value
            for key, value in payload.items()
            if key not in RAW_FIELD_EXCLUDE_KEYS
        }
    if not isinstance(fields, dict):
        raise ValueError("payload['fields'] must be a dict when present")

    for key, value in fields.items():
        if value is None:
            continue
        if isinstance(value, bool):
            point = point.field(str(key), value)
        elif isinstance(value, (int, float)):
            point = point.field(str(key), float(value))
        elif isinstance(value, str):
            point = point.field(str(key), value)
        else:
            point = point.field(str(key), json.dumps(value))

    timestamp = payload.get("timestamp")
    if timestamp is None:
        timestamp = fields.get("DATE_TIME") if isinstance(fields, dict) else None

    if timestamp:
        if isinstance(timestamp, str):
            parsed_timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        elif isinstance(timestamp, (int, float)):
            parsed_timestamp = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        else:
            raise ValueError("payload timestamp must be a string or unix epoch")
        point = point.time(parsed_timestamp, WritePrecision.NS)
    else:
        point = point.time(datetime.now(timezone.utc), WritePrecision.NS)

    return point

# -----------------------
# Message Handler
# -----------------------
def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        node_id = extract_node_id(payload, msg.topic)

        print(f"received node={node_id} topic={msg.topic}")
        print(json.dumps(payload, indent=2, sort_keys=True))

        point = build_point(payload)

        write_api.write(bucket=bucket, org=org, record=point)

        print(f"written: {node_id}")
        if node_id == "unknown":
            print(f"debug topic={msg.topic} keys={list(payload.keys())}")

    except Exception as e:
        print("error processing message:", e)

# -----------------------
# MQTT Setup (UPDATED API SAFE)
# -----------------------
def on_connect(client, userdata, flags, rc, properties=None):
    print("connected with code:", rc)
    client.subscribe(MQTT_TOPIC)

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_message = on_message

client.connect(MQTT_BROKER, MQTT_PORT, 60)

client.loop_forever()

