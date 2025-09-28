import time
import logging
import json
import os
from pyModbusTCP.client import ModbusClient
import paho.mqtt.client as mqtt
import threading
import sys
import signal

# Configure logging for service - write to file with rotation
from logging.handlers import RotatingFileHandler

# Determine base directory
if getattr(sys, 'frozen', False):
    # Running from exe
    BASE_DIR = os.path.dirname(sys.executable)
else:
    # Running from script
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Setup logging with rotation for service
log_file = os.path.join(BASE_DIR, "adam_advantech_service.log")
handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)

# Also log to console when running interactively
if not getattr(sys, 'frozen', False):
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    logger.addHandler(console_handler)

CONFIG_PATH = os.path.join(BASE_DIR, "config.json")

config_mtime = None
config = {}
topic_map = {}  # topic -> device_ip

# Service control flags
service_running = True
stop_current_mode = False

def signal_handler(signum, frame):
    """Handle service stop signals"""
    global service_running, stop_current_mode
    logging.info(f"Received signal {signum}, shutting down service...")
    service_running = False
    stop_current_mode = True

# Register signal handlers for service control
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

def load_config(mqtt_client=None):
    """Load config from file if changed, log errors, update MQTT subscriptions"""
    global config, config_mtime, topic_map, stop_current_mode, current_mode
    try:
        if not os.path.exists(CONFIG_PATH):
            logging.error(f"Config file not found: {CONFIG_PATH}")
            return False
            
        mtime = os.path.getmtime(CONFIG_PATH)
        if config_mtime != mtime:
            old_topic_map = topic_map.copy()
            old_config = config.copy()
            
            # Fix: Specify UTF-8 encoding to handle Chinese characters
            with open(CONFIG_PATH, "r", encoding='utf-8') as f:
                config = json.load(f)
            config_mtime = mtime
            logging.info("Configuration reloaded")

            # Validate mandatory keys
            mandatory_keys = ["ADAN_MQTT", "ACTIVE_THRESHOLD_BY_SECONDS", "DEVICE_IP_MAP", "MQTT_TOPICS"]
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

            # Check if ADAN_MQTT mode changed
            if old_config and old_config.get("ADAN_MQTT", "").upper() != config.get("ADAN_MQTT", "").upper():
                logging.info(f"ADAN_MQTT mode changed - switching modes")
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
                        break
                
                if mqtt_settings_changed:
                    logging.info("MQTT connection settings changed - reconnecting")
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
                        break
                
                if modbus_settings_changed:
                    logging.info("Modbus connection settings changed - restarting")
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
                
                # Find topics to subscribe
                topics_to_add = set(topic_map.keys()) - set(old_topic_map.keys())
                for topic in topics_to_add:
                    mqtt_client.subscribe(topic)
                    logging.info(f"Subscribed to {topic}")
        return True
    except UnicodeDecodeError as e:
        logging.error(f"Unicode decode error in config file (check encoding): {e}")
        return False
    except json.JSONDecodeError as e:
        logging.error(f"JSON decode error in config file: {e}")
        return False
    except Exception as e:
        logging.error(f"Error loading configuration: {e}")
        return False


di_active_since = {}   # {(device_ip, channel): start_time}
logged_events = set()  # {(device_ip, channel)}
mqtt_connected = False
last_connection_check = 0
current_mode = None  # Track current running mode


def adam_modbus_monitor():
    global stop_current_mode, current_mode, service_running
    current_mode = "MODBUS"
    stop_current_mode = False
    
    CHECK_INTERVAL = 1
    DI_CHANNELS = range(6)
    devices = {}  # Keep devices persistent to reuse connections

    while not stop_current_mode and service_running:
        if not load_config():
            logging.error("Failed to load config during Modbus monitor. Retrying...")
            time.sleep(5)
            continue

        # Check if we need to stop due to mode change or service stop
        if stop_current_mode or not service_running:
            logging.info("Stopping Modbus monitor")
            # Close all Modbus connections
            for ip, dev in devices.items():
                try:
                    if dev["client"].is_open:
                        dev["client"].close()
                except:
                    pass
            break

        # Get current config values
        ACTIVE_THRESHOLD_BY_SECONDS = config["ACTIVE_THRESHOLD_BY_SECONDS"]
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
            except:
                pass
        
        # Add new devices or update existing ones
        for name, ip in DEVICE_IP_MAP.items():
            if ip not in devices:
                devices[ip] = {"name": name, "client": ModbusClient(host=ip, port=modbus_port, auto_open=True)}
                logging.info(f"Added Modbus device: {ip} ({name})")
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

        for ip, dev in devices.items():
            if stop_current_mode or not service_running:  # Check again before each device
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
                                if key not in logged_events and time.time() - di_active_since[key] >= ACTIVE_THRESHOLD_BY_SECONDS:
                                    msg = f"ERROR!!!!!!! Modbus {ip} - DI{ch} active for {ACTIVE_THRESHOLD_BY_SECONDS} seconds"
                                    logging.info(msg)
                                    logged_events.add(key)
                        else:
                            if key in di_active_since:
                                del di_active_since[key]
                            if key in logged_events:
                                logged_events.remove(key)

            except Exception as e:
                logging.error(f"Error reading {ip}: {e}")

        if not stop_current_mode and service_running:
            time.sleep(CHECK_INTERVAL)


def mqtt_connection_monitor(client):
    """Monitor MQTT connection status, log disconnections, and check for config changes"""
    global mqtt_connected, last_connection_check, stop_current_mode, service_running
    
    while not stop_current_mode and service_running:
        current_time = time.time()
        
        # Check connection every 10 seconds
        if current_time - last_connection_check >= 10:
            last_connection_check = current_time
            
            if not mqtt_connected:
                logging.error("MQTT broker connection is down")
        
        # Check for configuration changes every 5 seconds
        try:
            load_config(client)
            if stop_current_mode or not service_running:
                logging.info("Stopping MQTT monitor")
                client.disconnect()
                break
        except Exception as e:
            logging.error(f"Error checking config in monitor thread: {e}")
        
        time.sleep(5)


def adam_mqtt_monitor():
    global mqtt_connected, stop_current_mode, current_mode, service_running
    current_mode = "MQTT"
    stop_current_mode = False
    
    def on_connect(client, userdata, flags, rc):
        global mqtt_connected
        if rc == 0:
            mqtt_connected = True
            logging.info(f"Connected to MQTT broker with code {rc}")
            load_config(client)  # Pass client to enable subscription management
            for topic in topic_map:
                client.subscribe(topic)
                logging.info(f"Subscribed to {topic}")
        else:
            mqtt_connected = False
            logging.error(f"Failed to connect to MQTT broker with code {rc}")

    def on_disconnect(client, userdata, rc):
        global mqtt_connected
        mqtt_connected = False
        if rc != 0 and not stop_current_mode and service_running:  # Don't log error if we initiated disconnect
            logging.error("MQTT broker unexpectedly disconnected")
        else:
            logging.info("MQTT broker disconnected normally")

    def on_message(client, userdata, msg):
        global di_active_since, logged_events
        # Don't reload config here to avoid frequent file I/O - it's handled by the monitor thread
        ACTIVE_THRESHOLD_BY_SECONDS = config.get("ACTIVE_THRESHOLD_BY_SECONDS", 5)

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
                        if key not in logged_events and time.time() - di_active_since[key] >= ACTIVE_THRESHOLD_BY_SECONDS:
                            msg_log = f"ERROR!!!!!!! MQTT {device_ip} - DI{ch - 1} active for {ACTIVE_THRESHOLD_BY_SECONDS} seconds"
                            logging.info(msg_log)
                            logged_events.add(key)
                else:
                    if key in di_active_since:
                        del di_active_since[key]
                    if key in logged_events:
                        logged_events.remove(key)
        except Exception as e:
            logging.error(f"Error processing message: {e}")

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
    while not stop_current_mode and service_running:
        try:
            mqtt_connected = False
            # Get fresh config values for each connection attempt
            MQTT_BROKER = config.get("MQTT_BROKER")
            MQTT_PORT = config.get("MQTT_PORT", 1883)
            
            logging.info(f"Attempting to connect to MQTT broker at {MQTT_BROKER}:{MQTT_PORT}")
            
            client.connect(MQTT_BROKER, MQTT_PORT, 60)
            client.loop_forever()
            
            # If loop_forever exits and we're not stopping, it means connection was lost
            if not stop_current_mode and service_running:
                mqtt_connected = False
            
        except ConnectionRefusedError as e:
            if not stop_current_mode and service_running:
                mqtt_connected = False
                logging.error(f"MQTT connection refused: {e}")
                time.sleep(10)
        except OSError as e:
            if not stop_current_mode and service_running:
                mqtt_connected = False
                logging.error(f"MQTT connection error (OSError): {e}")
                time.sleep(10)
        except Exception as e:
            if not stop_current_mode and service_running:
                mqtt_connected = False
                logging.error(f"MQTT connection error: {e}")
                time.sleep(10)
                
        if stop_current_mode or not service_running:
            break


if __name__ == "__main__":
    logging.info("Adam Advantech Service starting...")
    
    if not load_config():
        logging.error("Initial config load failed, exiting.")
        sys.exit(1)

    # Main loop to handle mode switching
    while service_running:
        stop_current_mode = False
        
        if config["ADAN_MQTT"].upper() == "TRUE":
            logging.info("Starting ADAM MQTT monitor")
            adam_mqtt_monitor()
        else:
            logging.info("Starting ADAM Modbus monitor")
            adam_modbus_monitor()
        
        # Wait a bit before checking config again
        if service_running:
            time.sleep(2)
            
            # Reload config to check for mode change
            if not load_config():
                logging.error("Failed to reload config, exiting.")
                break
    
    logging.info("Adam Advantech Service stopped.")