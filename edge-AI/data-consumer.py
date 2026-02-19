import paho.mqtt.client as mqtt
from confluent_kafka import Producer, Consumer
import joblib
import pandas as pd
import logging, os, json, time

DATA_COUNT = 3
DATA_TYPES = ("spO2", "hr", "activity")

REDPANDA_BROKER = os.getenv("RP_BROKER", "localhost:9092")
TOPIC_SUBSCRIBE = "patient.3.data"
TOPIC_PUBLISH = "patient.3.ai"
FEATURES = [
    "month", "age", "gender", "onset_to_baseline_m", "hr", 
    "spO2", "activity", "bmi", "fvc", "is_fast", "lag_alsfrs", "delta_month"
]


def main(consumer, producer, model):
  global logger
  try:
    while True:
      try:
        msg = consumer.poll(0.2)
        if msg is None: continue
        raw_value = msg.value().decode('utf-8')
        payload = json.loads(raw_value)
        input_df = pd.DataFrame([payload], columns = FEATURES)
        prediction = model.predict(input_df)[0]
        result = dict()
        score = float(prediction)
        result["alsfrs_r"] = score if score <= 40.0 else 40.0

        producer.produce(TOPIC_PUBLISH, value=json.dumps(result).encode('utf-8'))
        producer.flush()
        logger.info("Data sent to broker")
      except json.JSONDecodeError:
        logger.error(f"Message malformé reçu (pas du JSON) : {raw_value}")
        continue
      except Exception as e:
        logger.error(f"Erreur de traitement sur ce message : {e}")
        continue
  except Exception as e:
    logger.error(f"Erreur lors de l'inférence : {e}")


  

if __name__ == "__main__":
  logger = logging.getLogger("IoT-Agent du Edge setup")
  logging.basicConfig(
    level=logging.INFO,
    format="[%(name)s] [%(levelname)s] %(message)s"
  )

  config_c = {
    'bootstrap.servers': REDPANDA_BROKER,
    'group.id': 'mygroup',
  }

  config_p = {
    'bootstrap.servers': REDPANDA_BROKER,
    'acks': 'all'
  }

  for attempt in range(1,11):
    try:
      producer = Producer(config_p)
      consumer = Consumer(config_c)
      consumer.subscribe([TOPIC_SUBSCRIBE])
      logger.info(f" Connected to RedPanda at {REDPANDA_BROKER}")
      break
    except Exception as e:
      logger.warning(e)
      logger.warning(f"RedPanda not ready, retry {attempt}/{10}...")
      time.sleep(2)
  
  model = joblib.load("model/final_model.joblib")


  main(producer=producer, consumer=consumer, model=model)