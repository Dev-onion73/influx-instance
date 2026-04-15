import os
import json
import time
import paho.mqtt.client as mqtt

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import WriteOptions

# -----------------------
# InfluxDB Setup
# -----------------------
token = os.environ.get("INFLUXDB_TOKEN")  # FIXED
org = "solar"
bucket = "solar"
url = "http://10.0.40.101:8086"

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
MQTT_TOPIC = "solar/rtu/+"

# -----------------------
# Message Handler
# -----------------------
def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())

        device = payload.get("device", "unknown")

        point = (
            Point("solar_metrics")
            .tag("device", device)
            .field("voltage", float(payload.get("voltage", 0)))
            .field("current", float(payload.get("current", 0)))
            .field("power", float(payload.get("power", 0)))
            .field("temp", float(payload.get("temp", 0)))
            .time(time.time_ns(), WritePrecision.NS)
        )

        write_api.write(bucket=bucket, org=org, record=point)

        print(f"written: {device}")

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