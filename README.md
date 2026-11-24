# TP complet : IoT Air Quality Streaming Platform

## Kafka + Python + PostgreSQL + Airflow + Grafana + Docker Compose

**Niveau :** intermédiaire débutant en Data Engineering\
**Environnement :** Windows 10/11 + Docker Desktop + PowerShell\
**Durée indicative :** 8 à 12 heures\
**Important :** données simulées, non destinées à la surveillance réelle
ni à des décisions sanitaires.

------------------------------------------------------------------------

# 0. Architecture et objectifs

Nous simulons trois capteurs (centre-ville, zone industrielle, parc)
générant des mesures toutes les 2 secondes :

-   PM2.5 et PM10 en µg/m³
-   CO₂ en ppm
-   Température en °C
-   Humidité relative en %

Pipeline :

``` mermaid
flowchart TD
  A[Simulateur Python - 3 capteurs] -->|JSON| B[Kafka topic air_quality]
  B --> C[Consumer Python]
  C --> D{Validation}
  D -->|valide| E[(PostgreSQL raw measurements)]
  D -->|rejet| F[(PostgreSQL rejected measurements)]
  E --> G[Airflow - agrégation périodique]
  G --> H[(PostgreSQL analytics)]
  E --> I[Grafana Dashboard]
  H --> I
```

**Kafka** transporte les événements en streaming. **Consumer** reçoit et
valide les messages. **PostgreSQL** conserve les mesures et les rejets.
**Airflow** orchestre des traitements batch périodiques. **Grafana**
visualise les données.

------------------------------------------------------------------------

# 1. Prérequis

Installer Docker Desktop et vérifier dans PowerShell :

``` powershell
docker --version
docker compose version
```

Créer le projet :

``` powershell
mkdir C:\air-quality-lab
cd C:\air-quality-lab
mkdir dags
mkdir app
mkdir data
```

Structure finale :

``` text
C:\air-quality-lab
├── docker-compose.yaml
├── Dockerfile.airflow
├── Dockerfile.app
├── dags
│   └── air_quality_analytics.py
├── app
│   ├── producer.py
│   └── consumer.py
└── data
```

------------------------------------------------------------------------

# 2. Déployer Kafka, PostgreSQL, Airflow et Grafana

Créer `docker-compose.yaml` :

``` yaml
services:
  kafka:
    image: apache/kafka:3.9.1
    container_name: aq-kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_NUM_PARTITIONS: 3
    volumes:
      - kafka_data:/var/lib/kafka/data

  postgres:
    image: postgres:16
    container_name: aq-postgres
    environment:
      POSTGRES_USER: airuser
      POSTGRES_PASSWORD: airpass
      POSTGRES_DB: airquality
    ports:
      - "5433:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U airuser -d airquality"]
      interval: 5s
      timeout: 5s
      retries: 10

  consumer:
    build:
      context: .
      dockerfile: Dockerfile.app
    container_name: aq-consumer
    command: python -u /app/consumer.py
    depends_on:
      - kafka
      - postgres
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      KAFKA_TOPIC: air_quality
      KAFKA_GROUP_ID: air-quality-consumer
      PGHOST: postgres
      PGPORT: 5432
      PGDATABASE: airquality
      PGUSER: airuser
      PGPASSWORD: airpass
    restart: unless-stopped

  simulator:
    build:
      context: .
      dockerfile: Dockerfile.app
    container_name: aq-simulator
    command: python -u /app/producer.py
    depends_on:
      - kafka
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      KAFKA_TOPIC: air_quality
      INTERVAL_SECONDS: 2
    restart: unless-stopped

  airflow:
    build:
      context: .
      dockerfile: Dockerfile.airflow
    container_name: aq-airflow
    depends_on:
      - postgres
    environment:
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      AIRFLOW_CONN_AIRQUALITY_DB: postgresql://airuser:airpass@postgres:5432/airquality
    ports:
      - "8080:8080"
    volumes:
      - ./dags:/opt/airflow/dags
    command: standalone
    restart: unless-stopped

  grafana:
    image: grafana/grafana:11.5.2
    container_name: aq-grafana
    depends_on:
      - postgres
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_USER: admin
      GF_SECURITY_ADMIN_PASSWORD: admin
    volumes:
      - grafana_data:/var/lib/grafana

volumes:
  kafka_data:
  postgres_data:
  grafana_data:
```

## Explication

-   Kafka : broker Kafka en mode KRaft (sans ZooKeeper), configuration
    mono-nœud pour laboratoire.
-   PostgreSQL : base de données de mesures.
-   Consumer : programme Python qui consomme les messages Kafka.
-   Simulator : générateur de mesures.
-   Airflow : orchestration et analyses périodiques.
-   Grafana : visualisation.

**Configuration de laboratoire uniquement :** une seule instance Kafka
et des mots de passe simples. Ne pas utiliser ces paramètres en
production.

------------------------------------------------------------------------

# 3. Construire les images Python

## 3.1 Dockerfile du simulateur et Consumer

Créer `Dockerfile.app` :

``` dockerfile
FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir kafka-python psycopg2-binary
COPY app/ /app/
```

## 3.2 Dockerfile Airflow

Créer `Dockerfile.airflow` :

``` dockerfile
FROM apache/airflow:slim-3.4.0b2
RUN pip install --no-cache-dir apache-airflow-providers-postgres
```

Nous ajoutons le provider PostgreSQL pour que les DAGs puissent utiliser
`PostgresHook`.

------------------------------------------------------------------------

# 4. Créer le Kafka Topic

Démarrer les services infrastructurels :

``` powershell
docker compose up -d kafka postgres
```

Vérifier :

``` powershell
docker compose ps
docker compose logs kafka
```

Créer le topic :

``` powershell
docker exec -it aq-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic air_quality --partitions 3 --replication-factor 1
```

Lister les topics :

``` powershell
docker exec -it aq-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
```

Résultat attendu :

``` text
air_quality
```

Inspecter le topic :

``` powershell
docker exec -it aq-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic air_quality
```

Résultat attendu : topic `air_quality`, 3 partitions, replication factor
1.

**Topic :** catégorie ou flux logique de messages.\
**Partition :** subdivision permettant le parallélisme et l'ordre au
sein d'une partition.\
**Offset :** position d'un message dans une partition.

------------------------------------------------------------------------

# 5. Créer le IoT Simulator (Producer)

Créer `app/producer.py` :

``` python
import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "air_quality")
INTERVAL = float(os.getenv("INTERVAL_SECONDS", "2"))

SENSORS = [
    {"sensor_id": "S01", "location": "Centre_Ville"},
    {"sensor_id": "S02", "location": "Zone_Industrielle"},
    {"sensor_id": "S03", "location": "Parc"},
]

def generate_measurement(sensor):
    # Valeurs synthétiques avec variations aléatoires.
    # Le capteur industriel a tendance à produire des concentrations plus élevées.
    industrial = sensor["sensor_id"] == "S02"
    return {
        "sensor_id": sensor["sensor_id"],
        "location": sensor["location"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "pm2_5": round(random.uniform(10, 35) if not industrial else random.uniform(40, 120), 2),
        "pm10": round(random.uniform(15, 60) if not industrial else random.uniform(70, 180), 2),
        "co2": round(random.uniform(380, 650) if not industrial else random.uniform(600, 1200), 2),
        "temperature": round(random.uniform(18, 32), 2),
        "humidity": round(random.uniform(30, 75), 2),
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        retries=5,
    )
    print(f"Producer connected. Topic={TOPIC}")

    try:
        while True:
            for sensor in SENSORS:
                event = generate_measurement(sensor)
                try:
                    producer.send(TOPIC, key=event["sensor_id"].encode(), value=event)
                    producer.flush()
                    print("Sent:", event)
                except KafkaError as exc:
                    print("Kafka send error:", exc)
            time.sleep(INTERVAL)
    finally:
        producer.close()

if __name__ == "__main__":
    main()
```

Le simulateur génère des mesures artificielles et les envoie à Kafka.

## 5.1 Comprendre le Producer

-   `KafkaProducer` : client Python pour publier des messages.
-   `value_serializer` : transforme le dictionnaire en JSON puis en
    bytes.
-   `producer.send()` : publie un message.
-   `key` : identifiant du capteur; aide à garder les messages d'un
    capteur dans la même partition.
-   `flush()` : attend l'envoi des messages en attente.
-   `time.sleep()` : rythme de simulation.

------------------------------------------------------------------------

# 6. Créer le Consumer et les tables SQL

Créer `app/consumer.py` :

``` python
import json
import os
from datetime import datetime

import psycopg2
from kafka import KafkaConsumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "air_quality")
GROUP = os.getenv("KAFKA_GROUP_ID", "air-quality-consumer")

PG_CONFIG = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": int(os.getenv("PGPORT", "5432")),
    "dbname": os.getenv("PGDATABASE", "airquality"),
    "user": os.getenv("PGUSER", "airuser"),
    "password": os.getenv("PGPASSWORD", "airpass"),
}

DDL = """
CREATE TABLE IF NOT EXISTS measurements (
    id BIGSERIAL PRIMARY KEY,
    sensor_id TEXT NOT NULL,
    location TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    pm2_5 DOUBLE PRECISION NOT NULL,
    pm10 DOUBLE PRECISION NOT NULL,
    co2 DOUBLE PRECISION NOT NULL,
    temperature DOUBLE PRECISION NOT NULL,
    humidity DOUBLE PRECISION NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS rejected_measurements (
    id BIGSERIAL PRIMARY KEY,
    payload JSONB,
    rejection_reason TEXT,
    rejected_at TIMESTAMPTZ DEFAULT NOW()
);
"""

def valid_event(event):
    required = ["sensor_id", "location", "timestamp", "pm2_5", "pm10", "co2", "temperature", "humidity"]
    for key in required:
        if key not in event:
            return False, f"missing field: {key}"

    try:
        datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
        numeric = [float(event[k]) for k in ["pm2_5", "pm10", "co2", "temperature", "humidity"]]
    except (ValueError, TypeError):
        return False, "invalid type or timestamp"

    pm25, pm10, co2, temp, humidity = numeric
    if pm25 < 0 or pm10 < 0 or co2 < 0:
        return False, "negative pollutant value"
    if not -50 <= temp <= 80:
        return False, "temperature outside lab limits"
    if not 0 <= humidity <= 100:
        return False, "humidity outside 0-100"
    if pm25 > pm10:
        return False, "PM2.5 greater than PM10 (rule used for this lab)"
    return True, "ok"

def main():
    conn = psycopg2.connect(**PG_CONFIG)
    conn.autocommit = False
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    print("Consumer started")

    try:
        for message in consumer:
            event = message.value
            ok, reason = valid_event(event)

            with conn.cursor() as cur:
                if ok:
                    cur.execute("""
                        INSERT INTO measurements
                        (sensor_id, location, event_time, pm2_5, pm10, co2, temperature, humidity)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        event["sensor_id"], event["location"],
                        event["timestamp"], event["pm2_5"], event["pm10"],
                        event["co2"], event["temperature"], event["humidity"],
                    ))
                    print("Stored:", event["sensor_id"], event["timestamp"])
                else:
                    cur.execute("""
                        INSERT INTO rejected_measurements (payload, rejection_reason)
                        VALUES (%s, %s)
                    """, (json.dumps(event), reason))
                    print("Rejected:", reason)
            conn.commit()
    except KeyboardInterrupt:
        print("Consumer interrupted")
    finally:
        consumer.close()
        conn.close()

if __name__ == "__main__":
    main()
```

## 6.1 Data Quality rules

  -----------------------------------------------------------------------
  Règle                               Contrôle
  ----------------------------------- -----------------------------------
  Champs obligatoires                 Identifiant, lieu, date, mesures

  Timestamp                           Format date valide

  PM2.5, PM10, CO₂                    Non négatifs

  Température                         Entre -50 et 80 °C (limites
                                      pédagogiques)

  Humidité                            Entre 0 et 100 %

  PM2.5 ≤ PM10                        Cohérence pédagogique (pas une
                                      règle universelle de capteur)
  -----------------------------------------------------------------------

Les données rejetées ne sont pas supprimées silencieusement : elles sont
enregistrées dans `rejected_measurements`.

------------------------------------------------------------------------

# 7. Démarrer le Streaming

Construire et démarrer l'ensemble :

``` powershell
docker compose up -d --build
```

Vérifier les services :

``` powershell
docker compose ps
```

Suivre les logs du simulateur :

``` powershell
docker compose logs -f simulator
```

Exemple de résultat :

``` text
Producer connected. Topic=air_quality
Sent: {'sensor_id': 'S01', 'location': 'Centre_Ville', ...}
Sent: {'sensor_id': 'S02', 'location': 'Zone_Industrielle', ...}
Sent: {'sensor_id': 'S03', 'location': 'Parc', ...}
```

Suivre le Consumer :

``` powershell
docker compose logs -f consumer
```

Exemple :

``` text
Consumer started
Stored: S01 2026-...
Stored: S02 2026-...
Stored: S03 2026-...
```

------------------------------------------------------------------------

# 8. Vérifier les données dans PostgreSQL

Se connecter :

``` powershell
docker exec -it aq-postgres psql -U airuser -d airquality
```

Afficher les dernières mesures :

``` sql
SELECT * FROM measurements ORDER BY event_time DESC LIMIT 10;
```

Compter les mesures par capteur :

``` sql
SELECT sensor_id, location, COUNT(*) AS total
FROM measurements
GROUP BY sensor_id, location
ORDER BY sensor_id;
```

Exemple de résultat (le nombre dépend du temps d'exécution) :

``` text
 sensor_id | location           | total
-----------+--------------------+------
 S01      | Centre_Ville       | 120
 S02      | Zone_Industrielle  | 120
 S03      | Parc               | 120
```

Moyennes :

``` sql
SELECT
  sensor_id,
  ROUND(AVG(pm2_5)::numeric, 2) AS avg_pm25,
  ROUND(AVG(pm10)::numeric, 2) AS avg_pm10,
  ROUND(AVG(co2)::numeric, 2) AS avg_co2
FROM measurements
GROUP BY sensor_id;
```

Vérifier les rejets :

``` sql
SELECT * FROM rejected_measurements ORDER BY rejected_at DESC;
```

Quitter psql :

``` sql
\q
```

------------------------------------------------------------------------

# 9. Kafka : inspecter les messages

Lire les messages du topic :

``` powershell
docker exec -it aq-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic air_quality --from-beginning --max-messages 5
```

Résultat attendu : cinq messages JSON contenant les mesures. Cette
commande consomme également des messages; elle est utilisée ici pour
inspection.

Afficher les informations du topic :

``` powershell
docker exec -it aq-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic air_quality
```

------------------------------------------------------------------------

# 10. Airflow : traitement analytique périodique

Objectif : toutes les 5 minutes, agréger les mesures par capteur sur les
5 dernières minutes et stocker les résultats dans une table Analytics.

Créer `dags/air_quality_analytics.py` :

``` python
from datetime import datetime
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag(
    dag_id="air_quality_analytics",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["IoT", "Analytics"],
)
def air_quality_analytics():

    @task
    def aggregate():
        hook = PostgresHook(postgres_conn_id="airquality_db")
        conn = hook.get_conn()
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS air_quality_5min (
                window_end TIMESTAMPTZ,
                sensor_id TEXT,
                location TEXT,
                avg_pm2_5 NUMERIC,
                avg_pm10 NUMERIC,
                avg_co2 NUMERIC,
                avg_temperature NUMERIC,
                avg_humidity NUMERIC,
                readings_count INTEGER,
                PRIMARY KEY (window_end, sensor_id)
            )
        """)

        cur.execute("""
            INSERT INTO air_quality_5min (
                window_end, sensor_id, location,
                avg_pm2_5, avg_pm10, avg_co2,
                avg_temperature, avg_humidity, readings_count
            )
            SELECT
                date_trunc('minute', NOW()),
                sensor_id,
                location,
                AVG(pm2_5),
                AVG(pm10),
                AVG(co2),
                AVG(temperature),
                AVG(humidity),
                COUNT(*)
            FROM measurements
            WHERE event_time >= NOW() - INTERVAL '5 minutes'
            GROUP BY sensor_id, location
            ON CONFLICT (window_end, sensor_id)
            DO UPDATE SET
                avg_pm2_5 = EXCLUDED.avg_pm2_5,
                avg_pm10 = EXCLUDED.avg_pm10,
                avg_co2 = EXCLUDED.avg_co2,
                avg_temperature = EXCLUDED.avg_temperature,
                avg_humidity = EXCLUDED.avg_humidity,
                readings_count = EXCLUDED.readings_count
        """)

        conn.commit()
        cur.close()
        conn.close()
        print("5-minute aggregates updated")

    aggregate()

air_quality_analytics()
```

## 10.1 Pourquoi Airflow ?

Kafka fonctionne continuellement pour transporter les événements.
Airflow est adapté pour orchestrer des traitements planifiés, par
exemple :

-   agrégations périodiques;
-   rapports quotidiens;
-   vérification des capteurs silencieux;
-   contrôles et tâches de maintenance;
-   pipelines batch.

## 10.2 Démarrer Airflow

Airflow est inclus dans Compose. Accéder à :

``` text
http://localhost:8080
```

Récupérer le mot de passe initial :

``` powershell
docker exec aq-airflow airflow standalone
```

Si le service est déjà lancé, utiliser plutôt les logs :

``` powershell
docker compose logs airflow
```

Dans l'interface, trouver `air_quality_analytics`, l'activer et observer
ses runs.

------------------------------------------------------------------------

# 11. Grafana : Dashboard

Accéder à Grafana :

``` text
http://localhost:3000
```

Identifiants :

``` text
Username: admin
Password: admin
```

## 11.1 Ajouter PostgreSQL Data Source

Dans Grafana :

1.  Connections → Data sources.
2.  Add data source.
3.  PostgreSQL.
4.  Renseigner :

  Paramètre            Valeur
  -------------------- -----------------
  Host                 `postgres:5432`
  Database             `airquality`
  User                 `airuser`
  Password             `airpass`
  TLS/SSL Mode         `disable`
  PostgreSQL version   16
  TimescaleDB          désactivé

Puis Save & Test.

## 11.2 Panel : PM2.5 par capteur

Créer un Dashboard → Add visualization → PostgreSQL.

SQL :

``` sql
SELECT
  event_time AS time,
  sensor_id AS metric,
  pm2_5 AS value
FROM measurements
WHERE $__timeFilter(event_time)
ORDER BY event_time;
```

Choisir Time series.

## 11.3 Panel : CO₂

``` sql
SELECT
  event_time AS time,
  sensor_id AS metric,
  co2 AS value
FROM measurements
WHERE $__timeFilter(event_time)
ORDER BY event_time;
```

## 11.4 Panel : PM10

``` sql
SELECT
  event_time AS time,
  sensor_id AS metric,
  pm10 AS value
FROM measurements
WHERE $__timeFilter(event_time)
ORDER BY event_time;
```

## 11.5 Panel : dernière mesure par capteur

``` sql
SELECT DISTINCT ON (sensor_id)
  event_time AS time,
  sensor_id,
  pm2_5,
  pm10,
  co2,
  temperature,
  humidity
FROM measurements
ORDER BY sensor_id, event_time DESC;
```

Créer plusieurs panels : PM2.5, PM10, CO₂, température, humidité et
nombre de messages.

------------------------------------------------------------------------

# 12. Tests et exercices

## Exercice 1 --- Augmenter la fréquence

Modifier `INTERVAL_SECONDS` dans le Compose à 1 seconde.

Question : combien de messages sont générés par minute par les trois
capteurs ?

**Réponse attendue :** environ 180 messages/minute (3 capteurs × 60).

## Exercice 2 --- Ajouter un quatrième capteur

Ajouter dans `SENSORS` :

``` python
{"sensor_id": "S04", "location": "Campus"}
```

Question : quelles modifications sont nécessaires dans le code ?

## Exercice 3 --- Simuler un capteur défaillant

Modifier les données générées pour produire une humidité de 150%.

Question : le Consumer doit-il enregistrer la mesure dans `measurements`
ou dans `rejected_measurements` ?

**Résultat attendu :** la mesure doit être rejetée avec la règle de
validation actuelle.

## Exercice 4 --- Modifier Kafka

Créer le topic avec 5 partitions.

Question : pourquoi augmenter le nombre de partitions ?

## Exercice 5 --- SQL

Calculer le capteur qui présente la moyenne PM2.5 la plus élevée :

``` sql
SELECT sensor_id, location, AVG(pm2_5) AS avg_pm25
FROM measurements
GROUP BY sensor_id, location
ORDER BY avg_pm25 DESC
LIMIT 1;
```

## Exercice 6 --- Airflow

Modifier le DAG afin d'exécuter l'agrégation toutes les 10 minutes.

Indice : `schedule="*/10 * * * *"`.

## Exercice 7 --- Grafana

Créer un panel montrant le nombre de mesures par capteur :

``` sql
SELECT sensor_id, COUNT(*) AS readings
FROM measurements
WHERE $__timeFilter(event_time)
GROUP BY sensor_id
ORDER BY readings DESC;
```

------------------------------------------------------------------------

# 13. Dépannage

## Kafka n'est pas prêt

``` powershell
docker compose logs kafka
```

Attendre que Kafka démarre puis vérifier le topic.

## Consumer ne se connecte pas

``` powershell
docker compose logs consumer
```

Dans Docker, le broker est `kafka:9092`, pas `localhost:9092`.

## PostgreSQL

``` powershell
docker compose logs postgres
docker exec -it aq-postgres psql -U airuser -d airquality
```

## Airflow

``` powershell
docker compose logs airflow
docker exec -it aq-airflow airflow dags list
```

## DAG n'apparaît pas

Vérifier :

-   le fichier est dans `dags/`;
-   le code Python est valide;
-   les logs Airflow;
-   le provider PostgreSQL est installé dans l'image.

## Grafana ne se connecte pas

Utiliser `postgres:5432` comme host dans Grafana, pas `localhost:5433`.

------------------------------------------------------------------------

# 14. Arrêter et nettoyer

Arrêter les services :

``` powershell
docker compose down
```

Arrêter et supprimer également les volumes (supprime les données
PostgreSQL, Kafka et Grafana du lab) :

``` powershell
docker compose down -v
```

**Attention :** la commande `down -v` efface les volumes persistants et
les données stockées.

Redémarrer le projet sans supprimer les données :

``` powershell
docker compose up -d
```

------------------------------------------------------------------------

# 15. Architecture finale

``` text
                  +------------------------+
                  |   IoT Python Simulator |
                  | 3 sensors / JSON events|
                  +-----------+------------+
                              |
                              v
                  +------------------------+
                  |     Kafka Broker      |
                  | Topic: air_quality    |
                  +-----------+------------+
                              |
                              v
                  +------------------------+
                  |   Python Consumer     |
                  | Validation / Routing  |
                  +------+-----------+-----+
                         |           |
                 Valid   |           | Rejected
                         v           v
                  +-----------+  +----------------+
                  | PostgreSQL|  | Rejected table|
                  | measures  |  +----------------+
                  +-----+-----+
                        |
               +--------+-----------+
               |                    |
               v                    v
        +-------------+      +--------------+
        |  Airflow    |      |   Grafana    |
        | Aggregation |      | Visualization|
        +-------------+      +--------------+
```

------------------------------------------------------------------------

# 16. Conclusion

Ce TP a permis de construire une architecture IoT de Data Engineering
composée d'un simulateur de capteurs, d'un broker Kafka, d'un
consommateur Python, d'une base PostgreSQL, d'un orchestrateur Airflow
et d'un dashboard Grafana.

Nous avons exploré :

-   la génération de données IoT;
-   le streaming événementiel avec Kafka;
-   le traitement des messages en Python;
-   les contrôles de qualité;
-   le stockage relationnel;
-   l'orchestration de traitements périodiques;
-   la visualisation de données en temps réel.

**Extensions possibles :** MQTT avec Mosquitto, API FastAPI, alertes, ML
pour prédire la pollution, déploiement cloud, tests automatisés,
monitoring du Consumer, gestion de données en retard et sécurité Kafka.
