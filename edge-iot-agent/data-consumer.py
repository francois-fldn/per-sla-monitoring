import paho.mqtt.client as mqtt
import logging, os, json

DATA_COUNT = 3
DATA_TYPES = ("spO2", "hr", "activity")

def main():

  cur_json = dict()
  cnt = 0

  def on_message(client, userdata, message):
    nonlocal cur_json, cnt

    # aggregation des données
    data = message.payload.decode('utf-8')
    metric = message.topic.split('/')[1]
    logger.info(f"Message recu sur le topic {metric} : {data}")
    cur_json[metric] = float(data)

    # annotation des données
    cnt += 1
    if cnt == DATA_COUNT:
      cur_json["age"] = health_data["age"]
      cur_json["gender"] = health_data["gender"]
      logger.info(f"Data finale: {cur_json}")
      cur_json = dict()
      cnt = 0


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

  with open('dataset/peppapig/patient_health_info.json', 'r') as file:
    health_data = json.load(file)

  main()