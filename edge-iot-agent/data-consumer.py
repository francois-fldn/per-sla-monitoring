import paho.mqtt.client as mqtt
from confluent_kafka import Producer, KafkaError
from utils import json_formatter
from utils import cleaner
import logging, os, json, time

DATA_COUNT = 3
DATA_TYPES = ("spO2", "hr", "activity")

MQTT_BROKER_ADDRESS = os.getenv("BROKER_ADDRESS", "localhost")
MQTT_BROKER_PORT = int(os.getenv("BROKER_PORT", "1883"))
REDPANDA_BROKER = os.getenv("RP_BROKER","localhost:9092")
PATIENT = os.getenv("PATIENT_USER_NAME", "peppapig")

def main():

  cur_json = dict()
  cnt = 0

  def on_message(client, userdata, message):
    nonlocal cur_json, cnt

    # aggregation des données
    data = float(message.payload.decode('utf-8'))
    metric = message.topic.split('/')[1]
    logger.info(f"Message recu sur le topic {metric} : {data}")
    # cur_json[metric] = data
    

    # TODO : ajouter partie filtrage ici
    cleaned_data = cleaner.clean_value(data, metric)
    cur_json[metric] = cleaned_data
    # TODO : ajouter partie filtrage ici

    # annotation des données
    cnt += 1
    if cnt == DATA_COUNT:
      cur_json["age"] = health_data["age"]
      cur_json["gender"] = health_data["gender"]
      logger.info(f"Data finale: {cur_json}")
      final_json = json_formatter.convert_json(cur_json)

      topic = f"patient.{PATIENT_ID}.data"
      
      # print(final_json)
      producer.produce(topic, final_json, f"")
      producer.flush()
      logger.info(f"Donnees envoyees sur le topic {topic}")
      cur_json = dict()
      cnt = 0
      time.sleep(0.2)



  client = mqtt.Client()
  client.on_message = on_message

  client.connect(
    MQTT_BROKER_ADDRESS, 
    MQTT_BROKER_PORT, 
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

  config = {
    'bootstrap.servers': REDPANDA_BROKER,
    'acks': 'all'
  }

  for attempt in range(1,11):
    try:
      producer = Producer(config)
      logger.info(f" Connected to Kafka at {REDPANDA_BROKER}")
    except Exception as e:
      logger.warning(f"Kafka not ready, retry {attempt}/{10}...")
      time.sleep(2)

  with open(f'dataset/{PATIENT}/patient_health_info.json', 'r') as file:
    health_data = json.load(file)

  PATIENT_ID = health_data["ID"]

  main()