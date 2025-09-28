from pymodbus.client import ModbusTcpClient

# Change this to the IP address of your ADAM-6060
ADAM_IP = '192.168.1.99'
ADAM_PORT = 502

# Connect to ADAM-6060
client = ModbusTcpClient(ADAM_IP, port=ADAM_PORT, timeout=5)
connection = client.connect()

if connection:
    print("Connected to ADAM-6060")

    # Read digital inputs (Discrete Inputs: address 0-5)
    result = client.read_discrete_inputs(address=0, count=6)
    if not result.isError():
        print("Digital Inputs:", result.bits)
    else:
        print("Error reading inputs")

    # Toggle a digital output (e.g., output 0 ON)
    write_result = client.write_coil(address=1, value=True)
    if write_result.isError():
        print("Error writing output")
    else:
        print("Output 0 turned ON")

    client.close()
else:
    print("Failed to connect to ADAM-6060")
