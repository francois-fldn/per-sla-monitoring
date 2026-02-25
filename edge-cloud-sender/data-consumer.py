import requests
from confluent_kafka import Consumer, KafkaError
import logging, os, json, time

REDPANDA_BROKER = os.getenv("RP_BROKER","localhost:9092")

ORION_URL = "http://orion-ld:1026/ngsi-ld/v1/entityOperations/upsert"
HEADERS = {"Content-Type": "application/ld+json"}

print("En attente de données depuis Redpanda...")

def main(consumer):
  global logger
  try:
    while True:
      try:
        msg = consumer.poll(0.2)
        if msg is None: continue
        raw_value = msg.value().decode('utf-8')
        payload = json.loads(raw_value)
        
        payload = [payload] 
    
        try:
            # Envoi à Orion-LD
            response = requests.post(ORION_URL, json=payload, headers=HEADERS)
            print(f"✅ Envoyé à Orion ! Statut: {response.status_code}")
        except Exception as e:
            print(f"❌ Erreur de connexion à Orion : {e}")

      except Exception as e:
        logger.error(f"Erreur de traitement sur ce message : {e}")
        continue
  except Exception as e:
    logger.error(f"Erreur lors de l'inférence : {e}")

if __name__ == "__main__":
  config_c = {
    'bootstrap.servers': REDPANDA_BROKER,
    'group.id': 'mygroup',
  }


  for attempt in range(1,11):
    logger = logging.getLogger("IoT-Agent du Edge setup")
    logging.basicConfig(
      level=logging.INFO,
      format="[%(name)s] [%(levelname)s] %(message)s"
    )
    try:
      consumer = Consumer(config_c)
      consumer.subscribe(["patient.2.final_data"])
      logger.info(f" Connected to RedPanda at {REDPANDA_BROKER}")
      break
    except Exception as e:
      logger.warning(e)
      logger.warning(f"RedPanda not ready, retry {attempt}/{10}...")
      time.sleep(2)

  main(consumer)