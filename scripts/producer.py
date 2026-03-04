import os
import json
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

# 1. Charger les variables d'environnement depuis le fichier .env situé dans le dossier parent
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

# 2. Configuration
API_URL = os.getenv("API_URL", "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojsons")
# On utilise localhost car on lance le script depuis ton ordinateur (l'hôte)
KAFKA_BROKER = "localhost:9092" 
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "earthquakes")

def fetch_data():
    """Récupère les données des séismes depuis l'API de l'USGS."""
    print(f"Interrogation de l'API: {API_URL}")
    response = requests.get(API_URL)
    
    if response.status_code == 200:
        data = response.json()
        # L'USGS stocke la liste des séismes dans la clé 'features'
        return data.get('features', []) 
    else:
        print(f"Erreur lors de l'appel API. Code de statut: {response.status_code}")
        return []
def main():
    """Fonction principale pour ingérer les données dans Kafka."""
    print(f"Connexion au broker Kafka sur {KAFKA_BROKER}...")
    
    # Initialisation du Producer Kafka
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    # Récupération des données
    stations = fetch_data()
    
    if not stations:
        print("Aucune donnée à envoyer.")
        return

    # Envoi de chaque station (enregistrement) comme un message distinct dans Kafka
    print(f"Envoi de {len(stations)} enregistrements vers le topic '{KAFKA_TOPIC}'...")
    for station in stations:
        producer.send(KAFKA_TOPIC, value=station)
    
    # S'assurer que tous les messages sont bien partis
    producer.flush()
    print("Ingestion terminée avec succès !")

if __name__ == "__main__":
    main()