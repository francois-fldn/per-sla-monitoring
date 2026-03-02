# per-sla-monitoring

Ce repository contient le Proof-of-Concept de notre sujet de PER (PER2025-033) *"Suivi Individualisé de Patients atteints de la Maladie de Charcot (SLA) par Acquisition et Analyse de Données Capteurs"*. Le Proof-of-Concept simule la partie "individualisation" de notre architecture, du capteur vers le cloud.

Au niveau du Edge, projet rejoue les données d'un dataset vers un broker MQTT, agrege/nettoie/annote les données, envoie vers RedPanda, et un agent IA analyse les donnees pour calculer un score ALSFRS-R.
Au niveau du cloud, nous avons un subscriber RedPanda qui envoie vers une suite FiWare, qui est ensuite utilisée pour faire de l'analyse de données sur Grafana.

## Lancer le projet

Démarrer les services définis dans la racine :

```bash
docker compose up -d
```

Le fichier de configuration compose est [docker-compose.yml](docker-compose.yml).

## Arborescence et brève description des dossiers

- edge-AI  
  - Composant d'inférence qui consomme les données agrégées depuis Redpanda et prédit un score via un modèle entraîné. Point d'entrée et logique principale : [`edge_AI.data_consumer.main`](edge-AI/data-consumer.py) — voir [edge-AI/data-consumer.py](edge-AI/data-consumer.py).

- edge-cloud-sender  
  - Composant (envoyeur vers le cloud) — en charge d'éventuelles transmissions vers des endpoints cloud externes. (Dossier présent dans le repo root / docker-compose, contient la logique d'envoi.)

- edge-iot-agent  
  - Agent edge qui écoute le broker MQTT, nettoie/agrège les mesures et publie les messages vers Redpanda. Points pertinents :
    - logique de consommation/agrégation : [`edge_iot_agent.data_consumer.main`](edge-iot-agent/data-consumer.py) — voir [edge-iot-agent/data-consumer.py](edge-iot-agent/data-consumer.py)
    - utilitaires de nettoyage/filtrage : [`edge_iot_agent.utils.cleaner.median_filter`](edge-iot-agent/utils/cleaner.py) — voir [edge-iot-agent/utils/cleaner.py](edge-iot-agent/utils/cleaner.py)

- data-replay  
  - Script de rejeu qui lit les CSV des jeux de données et publie les valeurs sur les topics MQTT simulant des capteurs. Entrée principale : [`data_replay.main`](data-replay/data-replay.py) — voir [data-replay/data-replay.py](data-replay/data-replay.py)

- model  
  - Contient le modèle entraîné et ses métadonnées (`final_model.joblib`, `model_metadata.json`).

- mosquitto  
  - Configuration et volumes pour le broker MQTT utilisé localement par le setup (fichiers de config, logs, data).

- README et fichiers de configuration  
  - Le fichier principal de compose est [docker-compose.yml](docker-compose.yml).

## Endpoints / ports exposés (hosts sur localhost)

Les services exposés par docker-compose (ports hôtes :conteneurs) tels que définis dans configuré dans [docker-compose.yml](docker-compose.yml):

- Redpanda Console (UI) : http://localhost:8080

- Schema Registry (external) : http://localhost:18081  
- Pandaproxy / API HTTP Proxy : http://localhost:18082  
- Kafka (Redpanda) - API externe : localhost:9092  
- Redpanda Admin API : http://localhost:9644

- Mosquitto (MQTT broker) : localhost:1883

- MongoDB : localhost:27017

- Orion-LD (context broker) : http://localhost:1026

- Grafana : http://localhost:3001

## Fichiers clés

- [edge-cloud-sender](edge-cloud-sender/data-consumer.py) — RedPanda → FiWare
- [edge-AI/data-consumer.py](edge-AI/data-consumer.py) — inference / scoring
- [edge-iot-agent/data-consumer.py](edge-iot-agent/data-consumer.py) — MQTT → nettoyage → Redpanda
- [edge-iot-agent/utils/cleaner.py](edge-iot-agent/utils/cleaner.py) — fonctions de nettoyage (ex. `median_filter`)
- [data-replay/data-replay.py](data-replay/data-replay.py) — rejeu CSV → MQTT

## Notes rapides

- Les topics MQTT utilisés par le rejeu/agent sont du type `sensors/<metric>/data` ; l'agent souscrit à `sensors/+/data` (voir [edge-iot-agent/data-consumer.py](edge-iot-agent/data-consumer.py)).
- Les agrégations attendent 3 métriques (spO2, hr, activity) avant publication vers Redpanda (voir constantes `DATA_COUNT`, `DATA_TYPES` dans [edge-iot-agent/data-consumer.py](edge-iot-agent/data-consumer.py) et [edge-AI/data-consumer.py](edge-AI/data-consumer.py)).