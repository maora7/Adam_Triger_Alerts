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
    global config, config_mtime, topic_map, stop_current_mode, current_mode
    try:
        mtime = os.path.getmtime(CONFIG_PATH)
        if config_mtime != mtime:
            old_topic_map = topic_map.copy()
            old_config = config.copy()
            
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

            # Log all configuration changes
            if old_config:
                for key in config:
                    if key not in old_config or old_config[key] != config[key]:
                        logging.info(f"Config changed - {key}: {old_config.get(key, 'N/A')} -> {config[key]}")
                        print(f"Config changed - {key}: {old_config.get(key, 'N/A')} -> {config[key]}")

            # Check if ADAN_MQTT mode changed
            if old_config and old_config.get("ADAN_MQTT", "").upper() != config.get("ADAN_MQTT", "").upper():
                logging.info(f"ADAN_MQTT mode changed - switching modes")
                print(f"ADAN_MQTT mode changed - switching modes")
                stop_current_mode = True  # Signal to stop current mode
                return True  # Return early to trigger mode switch

            # Check if critical MQTT settings changed that require reconnection
            mqtt_critical_settings = ["MQTT_BROKER", "MQTT_PORT"]
            mqtt_settings_changed = False
            if old_config and current_mode == "MQTT":
                for setting in mqtt_critical_settings:
                    if old_config.get(setting) != config.get(setting):
                        mqtt_settings_changed = True
                        logging.info(f"Critical MQTT setting changed: {setting}")
                        print(f"Critical MQTT setting changed: {setting}")
                        break
                
                if mqtt_settings_changed:
                    logging.info("MQTT connection settings changed - reconnecting")
                    print("MQTT connection settings changed - reconnecting")
                    stop_current_mode = True
                    return True

            # Check if critical Modbus settings changed
            modbus_critical_settings = ["ADAM_PORT", "DEVICE_IP_MAP"]
            modbus_settings_changed = False
            if old_config and current_mode == "MODBUS":
                for setting in modbus_critical_settings:
                    if old_config.get(setting) != config.get(setting):
                        modbus_settings_changed = True
                        logging.info(f"Critical Modbus setting changed: {setting}")
                        print(f"Critical Modbus setting changed: {setting}")
                        break
                
                if modbus_settings_changed:
                    logging.info("Modbus connection settings changed - restarting")
                    print("Modbus connection settings changed - restarting")
                    stop_current_mode = True
                    return True

            # Build topic -> device_ip map
            topic_map = {}
            for topic in config["MQTT_TOPICS"]:
                device_id = topic.split("/")[1]
                if device_id in config["DEVICE_IP_MAP"]:
                    topic_map[topic] = config["DEVICE_IP_MAP"][device_id]
                else:
                    logging.warning(f"Topic {topic} has unknown device ID {device_id} and will be ignored")
            
            # Update MQTT subscriptions if client is provided and connected
            if mqtt_client and mqtt_connected and current_mode == "MQTT":
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
current_mode = None  # Track current running mode
stop_current_mode = False  # Flag to stop current mode


def adam_modbus_monitor():
    global stop_current_mode, current_mode
    current_mode = "MODBUS"
    stop_current_mode = False
    
    CHECK_INTERVAL = 1
    DI_CHANNELS = range(6)
    devices = {}  # Keep devices persistent to reuse connections

    while not stop_current_mode:
        if not load_config():
            logging.error("Failed to load config during Modbus monitor. Retrying...")
            time.sleep(5)
            continue

        # Check if we need to stop due to mode change
        if stop_current_mode:
            logging.info("Stopping Modbus monitor due to configuration change")
            print("Stopping Modbus monitor due to configuration change")
            # Close all Modbus connections
            for ip, dev in devices.items():
                try:
                    if dev["client"].is_open:
                        dev["client"].close()
                except:
                    pass
            break

        # Get current config values
        ACTIVE_THRESHOLD = config["ACTIVE_THRESHOLD"]
        DEVICE_IP_MAP = config["DEVICE_IP_MAP"]
        modbus_port = config["ADAM_PORT"]

        if not DEVICE_IP_MAP:
            logging.warning("No devices found in DEVICE_IP_MAP")
            time.sleep(CHECK_INTERVAL)
            continue

        # Update devices if IP map changed
        current_ips = set(DEVICE_IP_MAP.values())
        existing_ips = set(devices.keys())
        
        # Remove devices that are no longer in config
        for ip in existing_ips - current_ips:
            try:
                if devices[ip]["client"].is_open:
                    devices[ip]["client"].close()
                del devices[ip]
                logging.info(f"Removed Modbus device: {ip}")
                print(f"Removed Modbus device: {ip}")
            except:
                pass
        
        # Add new devices or update existing ones
        for name, ip in DEVICE_IP_MAP.items():
            if ip not in devices:
                devices[ip] = {"name": name, "client": ModbusClient(host=ip, port=modbus_port, auto_open=True)}
                logging.info(f"Added Modbus device: {ip} ({name})")
                print(f"Added Modbus device: {ip} ({name})")
            else:
                # Update name if changed
                devices[ip]["name"] = name
                # Update port if changed (recreate client)
                if devices[ip]["client"].port != modbus_port:
                    try:
                        devices[ip]["client"].close()
                    except:
                        pass
                    devices[ip]["client"] = ModbusClient(host=ip, port=modbus_port, auto_open=True)
                    logging.info(f"Updated Modbus port for {ip} to {modbus_port}")
                    print(f"Updated Modbus port for {ip} to {modbus_port}")

        for ip, dev in devices.items():
            if stop_current_mode:  # Check again before each device
                break
                
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

        if not stop_current_mode:
            time.sleep(CHECK_INTERVAL)


def mqtt_connection_monitor(client):
    """Monitor MQTT connection status, log disconnections, and check for config changes"""
    global mqtt_connected, last_connection_check, stop_current_mode
    
    while not stop_current_mode:
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
            if stop_current_mode:
                logging.info("Stopping MQTT monitor due to configuration change")
                print("Stopping MQTT monitor due to configuration change")
                client.disconnect()
                break
        except Exception as e:
            logging.error(f"Error checking config in monitor thread: {e}")
        
        time.sleep(5)


def adam_mqtt_monitor():
    global mqtt_connected, stop_current_mode, current_mode
    current_mode = "MQTT"
    stop_current_mode = False
    
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
        if rc != 0 and not stop_current_mode:  # Don't log error if we initiated disconnect
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
    while not stop_current_mode:
        try:
            mqtt_connected = False
            # Get fresh config values for each connection attempt
            MQTT_BROKER = config.get("MQTT_BROKER")
            MQTT_PORT = config.get("MQTT_PORT", 1883)
            
            logging.info(f"Attempting to connect to MQTT broker at {MQTT_BROKER}:{MQTT_PORT}")
            print(f"Attempting to connect to MQTT broker at {MQTT_BROKER}:{MQTT_PORT}")
            
            client.connect(MQTT_BROKER, MQTT_PORT, 60)
            client.loop_forever()
            
            # If loop_forever exits and we're not stopping, it means connection was lost
            if not stop_current_mode:
                mqtt_connected = False
            
        except ConnectionRefusedError as e:
            if not stop_current_mode:
                mqtt_connected = False
                logging.error(f"MQTT connection refused: {e}")
                print(f"MQTT connection refused: {e}")
                time.sleep(10)
        except OSError as e:
            if not stop_current_mode:
                mqtt_connected = False
                logging.error(f"MQTT connection error (OSError): {e}")
                print(f"MQTT connection error (OSError): {e}")
                time.sleep(10)
        except Exception as e:
            if not stop_current_mode:
                mqtt_connected = False
                logging.error(f"MQTT connection error: {e}")
                print(f"MQTT connection error: {e}")
                time.sleep(10)
                
        if stop_current_mode:
            break


if __name__ == "__main__":
    if not load_config():
        logging.error("Initial config load failed, exiting.")
        exit(1)

    # Main loop to handle mode switching
    while True:
        stop_current_mode = False
        
        if config["ADAN_MQTT"].upper() == "TRUE":
            print("Running ADAM MQTT monitor")
            logging.info("Starting ADAM MQTT monitor")
            adam_mqtt_monitor()
        else:
            print("Running ADAM Modbus monitor") 
            logging.info("Starting ADAM Modbus monitor")
            adam_modbus_monitor()
        
        # Wait a bit before checking config again
        time.sleep(2)
        
        # Reload config to check for mode change
        if not load_config():
            logging.error("Failed to reload config, exiting.")
            break