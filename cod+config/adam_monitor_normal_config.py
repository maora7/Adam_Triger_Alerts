import time
import logging
import json
import os
from pyModbusTCP.client import ModbusClient

# =========================
# Logging setup
# =========================
logging.basicConfig(
    filename="adam_advantech_log.txt",
    level=logging.INFO,
    format="%(asctime)s - %(message)s"
)

# =========================
# Load configuration
# =========================
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")
try:
    with open(CONFIG_PATH, "r") as f:
        config = json.load(f)
    ACTIVE_THRESHOLD = config["ACTIVE_THRESHOLD"]
    DEVICE_IP_MAP = config["DEVICE_IP_MAP"]
except KeyError as e:
    logging.error(f"Missing configuration key: {e}")
    raise
except Exception as e:
    logging.error(f"Error loading configuration: {e}")
    raise

# =========================
# Build devices dict
# =========================
devices = {}
for device_name, ip in DEVICE_IP_MAP.items():
    devices[ip] = {
        "name": device_name,
        "client": ModbusClient(host=ip, port=502, auto_open=True)
    }

# =========================
# State tracking
# =========================
di_active_since = {}   # {(device_ip, di_channel): start_time}
logged_events = set()  # {(device_ip, di_channel)}

CHECK_INTERVAL = 1
DI_CHANNELS = range(6)  # ADAM-6060 has 6 DI channels

# =========================
# Monitoring loop
# =========================
while True:
    for ip, dev in devices.items():
        client = dev["client"]
        if not client.is_open:
            client.open()

        try:
            di_status = client.read_discrete_inputs(0, 6)  # address 0-5
            if di_status:
                for ch in DI_CHANNELS:
                    key = (ip, ch)
                    if di_status[ch]:  # DI is active
                        if key not in di_active_since:
                            di_active_since[key] = time.time()
                        else:
                            if key not in logged_events and time.time() - di_active_since[key] >= ACTIVE_THRESHOLD:
                                msg = f"{ip} - DI{ch} active for {ACTIVE_THRESHOLD} seconds"
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
