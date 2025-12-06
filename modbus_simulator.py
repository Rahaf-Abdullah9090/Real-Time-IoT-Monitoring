# File: modbus_simulator.py
from pymodbus.server.sync import StartTcpServer
from pymodbus.device import ModbusDeviceIdentification
from pymodbus.datastore import ModbusSequentialDataBlock, ModbusSlaveContext, ModbusServerContext
import threading, struct, time, json, nest_asyncio
import paho.mqtt.client as mqtt

nest_asyncio.apply()

# ---- CONFIG ----
SLAVE_ID    = 1
MQTT_BROKER = "mosquitto"
MQTT_PORT   = 1883
MQTT_TOPIC  = "modbus/sensor1"

# ---- DATA STORE ----
store   = ModbusSlaveContext(hr=ModbusSequentialDataBlock(0, [0]*30))
context = ModbusServerContext(slaves={SLAVE_ID: store}, single=False)

identity = ModbusDeviceIdentification()
identity.VendorName        = 'IoT Demo'
identity.ProductCode       = 'DE'
identity.VendorUrl         = 'https://example.com'
identity.ProductName       = 'ModbusSim'
identity.ModelName         = 'ModbusSim'
identity.MajorMinorRevision= '1.0'

# ---- MQTT ----
mqtt_client = mqtt.Client()
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
mqtt_client.loop_start()

def float_to_regs(f):
    b = struct.pack('>f', f)
    return [
        int.from_bytes(b[0:2], 'big'),
        int.from_bytes(b[2:4], 'big')
    ]

# ---- MODBUS SERVER ----
def start_server():
    print("Modbus Simulator running on port 502")
    StartTcpServer(context, identity=identity, address=("0.0.0.0", 502))

threading.Thread(target=start_server, daemon=True).start()

# ---- DATA UPDATE LOOP ----
def update_loop():
    idx = 0
    while True:

        # Generate rotating values
        V_L1 = 220 + 10 * (idx % 3)
        I_L1 = 5 + (idx % 3)
        VA_L1 = V_L1 * I_L1
        P_L1 = 0.9 * VA_L1

        # Write values to Modbus holding registers
        context[SLAVE_ID].setValues(3, 1,  float_to_regs(V_L1))
        context[SLAVE_ID].setValues(3, 13, float_to_regs(I_L1))
        context[SLAVE_ID].setValues(3, 19, float_to_regs(VA_L1))
        context[SLAVE_ID].setValues(3, 25, float_to_regs(P_L1))

        # MQTT payload
        payload = {
            "device_id": "sensor1",
            "V_L1": V_L1,
            "I_L1": I_L1,
            "VA_L1": VA_L1,
            "P_L1": P_L1,
            "timestamp": int(time.time())
        }

        mqtt_client.publish(MQTT_TOPIC, json.dumps(payload))
        print("Published:", payload)

        idx += 1
        time.sleep(2)

threading.Thread(target=update_loop, daemon=True).start()

# ---- KEEP ALIVE ----
while True:
    time.sleep(1)