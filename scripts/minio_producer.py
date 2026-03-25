import os
import json
import requests
from io import BytesIO
from datetime import datetime
from minio import Minio
from dotenv import load_dotenv

# 1. Charger les variables d'environnement
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

# 2. Configuration API
API_URL = os.getenv("API_URL", "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojsons")

# 3. Configuration MinIO
# Assurez-vous que le port correspond à l'API de MinIO (9005 dans notre exemple précédent)
MINIO_ENDPOINT = "localhost:9005" 
MINIO_ACCESS_KEY = os.getenv("MINIO_USER", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_PASSWORD", "admin1234")
BUCKET_NAME = "bronze-earthquakes" # Le nom du dossier principal dans MinIO

def fetch_data():
    """Récupère les données des séismes depuis l'API de l'USGS."""
    print(f"Interrogation de l'API: {API_URL}")
    response = requests.get(API_URL)
    
    if response.status_code == 200:
        return response.json() # On récupère tout le JSON brut
    else:
        print(f"Erreur lors de l'appel API. Code: {response.status_code}")
        return None

def main():
    # Initialisation du client MinIO
    print(f"Connexion à MinIO sur {MINIO_ENDPOINT}...")
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False # Mis sur False car on est en local (HTTP et non HTTPS)
    )

    # Création du bucket (dossier) s'il n'existe pas déjà
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
        print(f"Bucket '{BUCKET_NAME}' créé avec succès.")
    
    # Récupération des données
    data = fetch_data()
    
    if not data:
        print("Aucune donnée à envoyer.")
        return

    # Préparation du fichier à envoyer
    # On génère un nom de fichier unique basé sur la date et l'heure
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    object_name = f"earthquakes_{timestamp}.json"
    
    # MinIO attend des "bytes" (des octets), on convertit donc notre dictionnaire JSON
    json_bytes = json.dumps(data).encode('utf-8')
    data_stream = BytesIO(json_bytes)
    data_size = len(json_bytes)

    # Envoi dans MinIO
    print(f"Envoi du fichier '{object_name}' vers MinIO...")
    client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=object_name,
        data=data_stream,
        length=data_size,
        content_type="application/json"
    )
    
    print("Ingestion dans MinIO terminée avec succès !")

if __name__ == "__main__":
    main()