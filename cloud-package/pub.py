import ssl
import json
import time
import paho.mqtt.client as mqtt

# -----------------------
# CONFIG
# -----------------------
ENDPOINT = "a2i2can38qdenj-ats.iot.eu-west-2.amazonaws.com"
PORT = 8883

CA_FILE = "AmazonRootCA1.pem"
CERT_FILE = "ec2.cert.pem"
KEY_FILE = "ec2.private.key"

TOPIC = "solar/test"

# -----------------------
# SETUP
# -----------------------
client = mqtt.Client(client_id="ec2-publisher")

client.tls_set(
    ca_certs=CA_FILE,
    certfile=CERT_FILE,
    keyfile=KEY_FILE,
    tls_version=ssl.PROTOCOL_TLSv1_2
)

client.connect(ENDPOINT, PORT, 60)

# -----------------------
# PUBLISH LOOP
# -----------------------
while True:
    payload = {
        "device": "rtu-1",
        "voltage": 24.5,
        "current": 3.1,
        "power": 75.0,
        "temp": 35.2
    }

    client.publish(TOPIC, json.dumps(payload))
    print("sent:", payload)

    time.sleep(2)