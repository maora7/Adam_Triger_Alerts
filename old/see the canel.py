from pymodbus.client import ModbusTcpClient

ADAM_IP = '192.168.1.99'
ADAM_PORT = 502

client = ModbusTcpClient(ADAM_IP, port=ADAM_PORT, timeout=5)
if client.connect():
    print("Connected to ADAM-6060")

    # Read 6 discrete inputs starting at address 0 (DI0 to DI5)
    result = client.read_discrete_inputs(address=0, count=6)
    if not result.isError():
        print("Digital Inputs status:", result.bits)
    else:
        print("Error reading digital inputs")

    client.close()
else:
    print("Failed to connect to ADAM-6060")
