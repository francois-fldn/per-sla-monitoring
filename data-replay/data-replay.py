import paho.mqtt.client as mqtt
import time, csv, os, logging

def on_publish(client, userdata, mid, reason_code, properties):
    print(f"Message {mid} envoyé avec succès !")

def main():
    with open(f'dataset/{patient}/sensor_data.csv', newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        for row in reader:
            ret= client.publish("sensors/hr/data", row[1])
            logger.info("Envoi de données sur le topic sensors/hr/data")
            ret= client.publish("sensors/spO2/data", row[2])
            logger.info("Envoi de données sur le topic sensors/spO2/data")
            ret= client.publish("sensors/activity/data", row[3])
            logger.info("Envoi de données sur le topic sensors/activity/data")
            time.sleep(0.5)

if __name__ == "__main__":
    logger = logging.getLogger("data replayer")
    logging.basicConfig(
        level= logging.INFO,
        format= "[%(name)s] [%(levelname)s] %(message)s"
    )

    logger.info("Connexion au broker mqtt")
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.on_publish = on_publish

    client.connect(
        os.getenv("BROKER_ADDRESS", "localhost"), 
        int(os.getenv("BROKER_PORT", "1883")), 
        60
    )
    logger.info("Connecté au broker MQTT")

    patient = os.getenv("PATIENT_USER_NAME", "peppapig")

    client.loop_start()
    main()
    client.loop_stop()
    client.disconnect()
    print("Fin du test.")