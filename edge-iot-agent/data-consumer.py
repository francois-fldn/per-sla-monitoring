import paho.mqtt.client as mqtt
import logging, os


def main():

  def on_message(client, userdata, message):
    data = message.payload.decode('utf-8')
    metric = message.topic.split('/')[1]
    logger.info(f"Message recu sur le topic {metric} : {data}")

  client = mqtt.Client()
  client.on_message = on_message

  client.connect(
    os.getenv("BROKER_ADDRESS", "localhost"), 
    int(os.getenv("BROKER_PORT", "1883")), 
    60
  )
  client.subscribe("sensors/+/data")
  client.loop_forever()

if __name__ == "__main__":
  logger = logging.getLogger("IoT-Agent du Edge setup")
  logging.basicConfig(
    level=logging.INFO,
    format="[%(name)s] [%(levelname)s] %(message)s"
  )
  main()