
import asyncio
import json
import os
import requests
from aiomqtt import Client
import RPi.GPIO as GPIO
from dotenv import load_dotenv

# Load environment variables
load_dotenv("/home/raspberrypi2/mqtt_cred.env")

GPIO.setwarnings(False)

RELAY_PIN = 10
GPIO.setmode(GPIO.BCM)
GPIO.setup(RELAY_PIN, GPIO.OUT)
GPIO.output(RELAY_PIN, GPIO.HIGH)

current_state = "OFF"

# Load from .env
BROKER = os.getenv("BROKER")
PORT = int(os.getenv("PORT"))
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
DEVICE_ID = os.getenv("DEVICE_ID")
COMMON_TOKEN = os.getenv("COMMON_TOKEN")

print("BROKER:", BROKER)
print("PORT:", PORT)
print("TOKEN:", ACCESS_TOKEN)
print("DEVICE:", DEVICE_ID)

ATTR_SUB_TOPIC = "v1/devices/me/attributes"
ATTR_REQ_TOPIC = "v1/devices/me/attributes/request/1"
TELEMETRY_TOPIC = "v1/devices/me/telemetry"


# HTTP function (runs in thread)
def send_to_common_device(relay, uuid, device):
    url = f"http://{BROKER}:8080/api/v1/{COMMON_TOKEN}/attributes"

    payload = {
        device: {
            "relay": relay,
            "uuid": uuid,
            "status": "success"
        }
    }

    try:
        res = requests.post(url, json=payload, timeout=5)
        print("HTTP sent:", res.status_code)
    except Exception as e:
        print("HTTP error:", e)


async def main():
    global current_state

    while True:
        try:
            async with Client(
                hostname=BROKER,
                port=PORT,
                username=ACCESS_TOKEN,
                password=None,
                keepalive=60
            ) as client:

                print("Connected to ThingsBoard")

                await client.subscribe(ATTR_SUB_TOPIC)

                # Request initial state
                await client.publish(
                    ATTR_REQ_TOPIC,
                    json.dumps({"sharedKeys": "relay"})
                )

                async for message in client.messages:
                    try:
                        data = json.loads(message.payload.decode())
                        print("RX:", data)

                        # Handle payload
                        if "shared" in data:
                            payload = data["shared"]
                        else:
                            payload = data

                        #  Single JSON format
                        inner = payload.get("relay", {})

                        command = inner.get("relay")
                        uuid = inner.get("uuid")
                        device = inner.get("device")

                        # Filter device
                        if device != DEVICE_ID:
                            print("Ignored: not my device")
                            continue  
                        
                        if not command:
                            continue

                        command = str(command).upper()

                        if command not in ["ON", "OFF"]:
                            continue

                        # Control relay
                        GPIO.output(RELAY_PIN, GPIO.LOW if command == "ON" else GPIO.HIGH)
                        current_state = command.lower()

                        print(f"Relay {command}, UUID={uuid}, DEVICE={device}")

                        # MQTT telemetry (optional)
                        await client.publish(
                          TELEMETRY_TOPIC,
                          json.dumps({
                          DEVICE_ID: {
                             "relay": current_state,
                             "uuid": uuid,
                             "status": "success"
                            }
                          })
                        )

                        #  Send to common device (correct async usage)
                        if uuid and device:
                            await asyncio.to_thread(
                                send_to_common_device,
                                current_state,
                                uuid,
                                DEVICE_ID
                            )

                    except Exception as e:
                        print("Error:", e)

        except Exception as e:
            print("MQTT reconnecting...", e)
            await asyncio.sleep(5)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally: 
        GPIO.cleanup()
