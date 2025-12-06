# modbus-simulator/Dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY modbus_simulator.py .

RUN pip install pymodbus paho-mqtt nest_asyncio

CMD ["python", "modbus_simulator.py"]
