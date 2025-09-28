import time
import logging
import json
import paho.mqtt.client as mqtt

# Setup logging
logging.basicConfig(
    filename="adam_advantech_log.txt",
    level=logging.INFO,
    format="%(asctime)s - %(message)s"
)

# Track DI states and timers
di_active_since = {}   # {(device_id, channel): start_time}
logged_events = set()  # {(device_id, channel)}

ACTIVE_THRESHOLD = 5  # seconds

# MQTT setup
MQTT_BROKER = "192.168.1.22"   # your broker IP
MQTT_PORT = 1883
MQTT_TOPICS = [
    "Advantech/74FE48905B08/data",
    "Advantech/74FE489052BF/data"
]

# Map device IDs to IPs (add more here if needed)
DEVICE_IPS = {
    "74FE48905B08": "192.168.1.101",
    "74FE489052BF": "192.168.1.99"
}


def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker with code", rc)
    for topic in MQTT_TOPICS:
        client.subscribe(topic)
        print(f"Subscribed to {topic}")


def on_message(client, userdata, msg):
    global di_active_since, logged_events

    try:
        payload = msg.payload.decode().strip()
        device_id = msg.topic.split("/")[1]   # e.g. "74FE48905B08"
        device_ip = DEVICE_IPS.get(device_id, device_id)  # fallback to device_id

        data = json.loads(payload)  # Parse JSON from Advantech

        # Loop through DI1..DI6
        for ch in range(1, 7):
            key = (device_id, f"di{ch}")
            if data.get(f"di{ch}", False):  # DI is true
                if key not in di_active_since:
                    di_active_since[key] = time.time()
                else:
                    if (key not in logged_events and
                            time.time() - di_active_since[key] >= ACTIVE_THRESHOLD):
                        msg_log = f"{device_ip} - DI{ch -1} active for {ACTIVE_THRESHOLD} seconds"
                        logging.info(msg_log)
                        print(msg_log)
                        logged_events.add(key)
            else:
                # Reset when inactive
                if key in di_active_since:
                    del di_active_since[key]
                if key in logged_events:
                    logged_events.remove(key)

    except Exception as e:
        print(f"Error processing message: {e}")


# Run MQTT client
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(MQTT_BROKER, MQTT_PORT, 60)
client.loop_forever()
