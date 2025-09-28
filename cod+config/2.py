import time
import logging
import json
import os
from pyModbusTCP.client import ModbusClient
import paho.mqtt.client as mqtt
import threading

logging.basicConfig(
    filename="adam_advantech_log.txt",
    level=logging.INFO,
    format="%(asctime)s - %(message)s"
)


CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")
config_mtime = None
config = {}
topic_map = {}  # topic -> device_ip

def load_config(mqtt_client=None):
    """Load config from file if changed, log errors, update MQTT subscriptions"""
    global config, config_mtime, topic_map
    try:
        mtime = os.path.getmtime(CONFIG_PATH)
        if config_mtime != mtime:
            old_topic_map = topic_map.copy()
            
            with open(CONFIG_PATH, "r") as f:
                config = json.load(f)
            config_mtime = mtime
            logging.info("Configuration reloaded")
            print("Configuration reloaded")

            # Validate mandatory keys
            mandatory_keys = ["ADAN_MQTT", "ACTIVE_THRESHOLD", "DEVICE_IP_MAP", "MQTT_TOPICS"]
            for key in mandatory_keys:
                if key not in config:
                    msg = f"Missing mandatory config key: {key}"
                    logging.error(msg)
                    raise KeyError(msg)

            # Build topic -> device_ip map
            topic_map = {}
            for topic in config["MQTT_TOPICS"]:
                device_id = topic.split("/")[1]
                if device_id in config["DEVICE_IP_MAP"]:
                    topic_map[topic] = config["DEVICE_IP_MAP"][device_id]
                else:
                    logging.warning(f"Topic {topic} has unknown device ID {device_id} and will be ignored")
            
            # Update MQTT subscriptions if client is provided and connected
            if mqtt_client and mqtt_connected:
                # Find topics to unsubscribe
                topics_to_remove = set(old_topic_map.keys()) - set(topic_map.keys())
                for topic in topics_to_remove:
                    mqtt_client.unsubscribe(topic)
                    logging.info(f"Unsubscribed from {topic}")
                    print(f"Unsubscribed from {topic}")
                
                # Find topics to subscribe
                topics_to_add = set(topic_map.keys()) - set(old_topic_map.keys())
                for topic in topics_to_add:
                    mqtt_client.subscribe(topic)
                    logging.info(f"Subscribed to {topic}")
                    print(f"Subscribed to {topic}")
        return True
    except json.JSONDecodeError as e:
        logging.error(f"JSON decode error in config file: {e}")
        print(f"JSON decode error: {e}")
        return False
    except Exception as e:
        logging.error(f"Error loading configuration: {e}")
        print(f"Error loading configuration: {e}")
        return False


di_active_since = {}   # {(device_ip, channel): start_time}
logged_events = set()  # {(device_ip, channel)}
mqtt_connected = False
last_connection_check = 0


def adam_modbus_monitor():
    CHECK_INTERVAL = 1
    DI_CHANNELS = range(6)

    while True:
        if not load_config():
            logging.error("Failed to load config during Modbus monitor. Retrying...")
            time.sleep(5)
            continue

        ACTIVE_THRESHOLD = config["ACTIVE_THRESHOLD"]
        DEVICE_IP_MAP = config["DEVICE_IP_MAP"]

        if not DEVICE_IP_MAP:
            logging.warning("No devices found in DEVICE_IP_MAP")
            time.sleep(CHECK_INTERVAL)
            continue

        devices = {}
        modbus_port = config["ADAM_PORT"]  
        for name, ip in DEVICE_IP_MAP.items():
            devices[ip] = {"name": name, "client": ModbusClient(host=ip, port=modbus_port, auto_open=True)}

        for ip, dev in devices.items():
            client = dev["client"]
            try:
                if not client.is_open:
                    client.open()

                di_status = client.read_discrete_inputs(0, 6)
                if di_status:
                    for ch in DI_CHANNELS:
                        key = (ip, ch)
                        if di_status[ch]:
                            if key not in di_active_since:
                                di_active_since[key] = time.time()
                            else:
                                if key not in logged_events and time.time() - di_active_since[key] >= ACTIVE_THRESHOLD:
                                    msg = f"Modbus {ip} - DI{ch} active for {ACTIVE_THRESHOLD} seconds"
                                    logging.info(msg)
                                    print(msg)
                                    logged_events.add(key)
                        else:
                            if key in di_active_since:
                                del di_active_since[key]
                            if key in logged_events:
                                logged_events.remove(key)

            except Exception as e:
                logging.error(f"Error reading {ip}: {e}")
                print(f"Error reading {ip}: {e}")

        time.sleep(CHECK_INTERVAL)


def mqtt_connection_monitor(client):
    """Monitor MQTT connection status, log disconnections, and check for config changes"""
    global mqtt_connected, last_connection_check
    
    while True:
        current_time = time.time()
        
        # Check connection every 10 seconds
        if current_time - last_connection_check >= 10:
            last_connection_check = current_time
            
            if not mqtt_connected:
                logging.error("MQTT broker connection is down")
                print("MQTT broker connection is down")
        
        # Check for configuration changes every 5 seconds
        try:
            load_config(client)
        except Exception as e:
            logging.error(f"Error checking config in monitor thread: {e}")
        
        time.sleep(5)


def adam_mqtt_monitor():
    global mqtt_connected
    
    def on_connect(client, userdata, flags, rc):
        global mqtt_connected
        if rc == 0:
            mqtt_connected = True
            print("Connected to MQTT broker with code", rc)
            logging.info(f"Connected to MQTT broker with code {rc}")
            load_config(client)  # Pass client to enable subscription management
            for topic in topic_map:
                client.subscribe(topic)
                print(f"Subscribed to {topic}")
                logging.info(f"Subscribed to {topic}")
        else:
            mqtt_connected = False
            logging.error(f"Failed to connect to MQTT broker with code {rc}")
            print(f"Failed to connect to MQTT broker with code {rc}")

    def on_disconnect(client, userdata, rc):
        global mqtt_connected
        mqtt_connected = False
        if rc != 0:
            logging.error("MQTT broker unexpectedly disconnected")
            print("MQTT broker unexpectedly disconnected")
        else:
            logging.info("MQTT broker disconnected normally")
            print("MQTT broker disconnected normally")

    def on_message(client, userdata, msg):
        global di_active_since, logged_events
        # Don't reload config here to avoid frequent file I/O - it's handled by the monitor thread
        ACTIVE_THRESHOLD = config.get("ACTIVE_THRESHOLD", 5)

        try:
            payload = msg.payload.decode().strip()
            if msg.topic not in topic_map:
                return

            device_ip = topic_map[msg.topic]
            data = json.loads(payload)

            for ch in range(1, 7):
                key = (device_ip, f"di{ch}")
                if data.get(f"di{ch}", False):
                    if key not in di_active_since:
                        di_active_since[key] = time.time()
                    else:
                        if key not in logged_events and time.time() - di_active_since[key] >= ACTIVE_THRESHOLD:
                            msg_log = f"MQTT {device_ip} - DI{ch - 1} active for {ACTIVE_THRESHOLD} seconds"
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
    client.on_disconnect = on_disconnect
    
    # Set keepalive to a shorter interval for faster detection
    client.keepalive = 30

    load_config()
    MQTT_BROKER = config.get("MQTT_BROKER")
    MQTT_PORT = config.get("MQTT_PORT", 1883)

    # Start connection monitor thread
    monitor_thread = threading.Thread(target=mqtt_connection_monitor, args=(client,), daemon=True)
    monitor_thread.start()

    # Handle connection errors with retry logic
    while True:
        try:
            mqtt_connected = False
            logging.info(f"Attempting to connect to MQTT broker at {MQTT_BROKER}:{MQTT_PORT}")
            print(f"Attempting to connect to MQTT broker at {MQTT_BROKER}:{MQTT_PORT}")
            
            client.connect(MQTT_BROKER, MQTT_PORT, 60)
            client.loop_forever()
            
        except ConnectionRefusedError as e:
            mqtt_connected = False
            logging.error(f"MQTT connection refused: {e}")
            print(f"MQTT connection refused: {e}")
            time.sleep(10)
        except OSError as e:
            mqtt_connected = False
            logging.error(f"MQTT connection error (OSError): {e}")
            print(f"MQTT connection error (OSError): {e}")
            time.sleep(10)
        except Exception as e:
            mqtt_connected = False
            logging.error(f"MQTT connection error: {e}")
            print(f"MQTT connection error: {e}")
            time.sleep(10)


if __name__ == "__main__":
    if not load_config():
        logging.error("Initial config load failed, exiting.")
        exit(1)

    if config["ADAN_MQTT"].upper() == "TRUE":
        print("Running ADAM MQTT monitor")
        adam_mqtt_monitor()
    else:
        print("Running ADAM Modbus monitor")
        adam_modbus_monitor()