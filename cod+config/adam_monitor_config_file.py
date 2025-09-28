import time
import logging
import json
import paho.mqtt.client as mqtt
import os

logging.basicConfig(
    filename="adam_advantech_log.txt",
    level=logging.INFO,
    format="%(asctime)s - %(message)s"
)

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")
config_mtime = None  # To track last modification time
config = {}

def load_config():
    global config, config_mtime
    try:
        mtime = os.path.getmtime(CONFIG_PATH)
        if config_mtime != mtime:
            with open(CONFIG_PATH, "r") as f:
                config = json.load(f)
            config_mtime = mtime
            logging.info("Configuration reloaded")
    except Exception as e:
        logging.error(f"Error loading configuration: {e}")
        print(f"Error loading configuration: {e}")

# Initial load
load_config()

# Track DI states and timers
di_active_since = {}   # {(device_ip, channel): start_time}
logged_events = set()  # {(device_ip, channel)}

def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker with code", rc)
    load_config()  # Load latest config on connect
    for topic in config.get("MQTT_TOPICS", []):
        client.subscribe(topic)
        print(f"Subscribed to {topic}")

def on_message(client, userdata, msg):
    global di_active_since, logged_events

    load_config()  # Reload config if changed
    ACTIVE_THRESHOLD = config.get("ACTIVE_THRESHOLD", 5)
    DEVICE_IP_MAP = config.get("DEVICE_IP_MAP", {})

    try:
        payload = msg.payload.decode().strip()
        device_id = msg.topic.split("/")[1]
        device_ip = DEVICE_IP_MAP.get(device_id, device_id)

        data = json.loads(payload)

        for ch in range(1, 7):
            key = (device_ip, f"di{ch}")
            if data.get(f"di{ch}", False):
                if key not in di_active_since:
                    di_active_since[key] = time.time()
                else:
                    if (key not in logged_events and
                        time.time() - di_active_since[key] >= ACTIVE_THRESHOLD):
                        msg_log = f"{device_ip} - DI{ch - 1} active for {ACTIVE_THRESHOLD} seconds"
                        logging.info(msg_log)
                        print(msg_log)
                        logged_events.add(key)
            else:
                if key in di_active_since:
                    del di_active_since[key]
                if key in logged_events:
                    logged_events.remove(key)

    except Exception as e:
        logging.error(f"Error processing message: {e}")
        print(f"Error processing message: {e}")

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(config.get("MQTT_BROKER", "127.0.0.1"), config.get("MQTT_PORT", 1883), 60)
client.loop_forever()
