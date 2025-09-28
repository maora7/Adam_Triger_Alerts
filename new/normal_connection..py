import time
import logging
from pyModbusTCP.client import ModbusClient

# Setup logging
logging.basicConfig(filename="adam_di_log.txt", level=logging.INFO, format="%(asctime)s - %(message)s")

# Devices to monitor
devices = {
    "192.168.1.100": {"name": "sys", "client": ModbusClient(host="192.168.1.101", port=502, auto_open=True)},
    "192.168.1.99":  {"name": "cuont", "client": ModbusClient(host="192.168.1.99",  port=502, auto_open=True)},
}

# State tracking
di_active_since = {}   # {(device_ip, di_channel): start_time}
logged_events = set()  # {(device_ip, di_channel)}

CHECK_INTERVAL = 1     # seconds
DI_CHANNELS = range(6) # ADAM-6060 has 6 DI channels
time_to_send = 5  # seconds to wait before logging active DI


while True:
    for ip, dev in devices.items():
        client = dev["client"]
        if not client.is_open:
            client.open()

        try:
            # Read DI status (6 inputs in one read)
            di_status = client.read_discrete_inputs(0, 6)  # address 0-5
            if di_status:
                for ch in DI_CHANNELS:
                    key = (ip, ch)
                    if di_status[ch]:  # DI is active
                        if key not in di_active_since:
                            di_active_since[key] = time.time()
                        else:
                            if key not in logged_events and time.time() - di_active_since[key] >= time_to_send:
                                msg = f"{dev['name']} ({ip}) - DI{ch} active for {time_to_send}+ seconds"
                                logging.info(msg)
                                print(msg)
                                logged_events.add(key)
                    else:
                        # Reset timer if input goes inactive
                        if key in di_active_since:
                            del di_active_since[key]
                        if key in logged_events:
                            logged_events.remove(key)
        except Exception as e:
            print(f"Error reading {ip}: {e}")

    time.sleep(CHECK_INTERVAL)
