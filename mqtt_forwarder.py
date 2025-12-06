import json
import paho.mqtt.client as mqtt
from kafka import KafkaProducer

# -------------------------------------------------
# MQTT CONFIG
# -------------------------------------------------
MQTT_BROKER = "mosquitto"
MQTT_PORT   = 1883
MQTT_TOPIC  = "modbus/sensor1"

# -------------------------------------------------
# KAFKA CONFIG
# -------------------------------------------------
KAFKA_BROKER = "kafka:9092"   # MUST be kafka:9092 for Docker DNS
KAFKA_TOPIC  = "sensor-data"

# -------------------------------------------------
# SETUP KAFKA PRODUCER
# -------------------------------------------------
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    api_version=(0, 10)   # forces compatibility for all brokers
)

print("🚀 MQTT → Kafka Forwarder Started")
print(f"📌 Listening to MQTT topic: {MQTT_TOPIC}")
print(f"📌 Forwarding to Kafka topic: {KAFKA_TOPIC}")
print("-------------------------------------------------\n")


# -------------------------------------------------
# MQTT MESSAGE HANDLER
# -------------------------------------------------
def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        print("📥 Received from MQTT:", payload)

        # Forward to Kafka
        producer.send(KAFKA_TOPIC, payload)
        producer.flush()

        print("📤 Sent to Kafka:", payload)

    except Exception as e:
        print("❌ ERROR forwarding to Kafka:", e)


# -------------------------------------------------
# SETUP MQTT CLIENT
# -------------------------------------------------
mqtt_client = mqtt.Client()
mqtt_client.on_message = on_message

mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
mqtt_client.subscribe(MQTT_TOPIC)

# -------------------------------------------------
# START LOOP
# -------------------------------------------------
try:
    mqtt_client.loop_forever()
except KeyboardInterrupt:
    print("🛑 Stopping forwarder...")
