import requests
import time
import json
import os
from google.cloud import storage
from datetime import datetime, timedelta

BASE_URL = "https://www.geodair.fr/api-ext"
PROJECT_ID = "pipelinequaliteair"

# --- FONCTIONS UTILITAIRES ---
def get_yesterday_date():
    """Renvoie la date d'hier (YYYY-MM-DD)."""
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


def get_pollutants_ref(bucket_name, ref_path="referentiel/polluants.json"):
    """Télécharge et parse le fichier référentiel JSON depuis GCS."""
    client = storage.Client(PROJECT_ID)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(ref_path)

    if not blob.exists():
        raise FileNotFoundError(f"Référentiel introuvable : gs://{bucket_name}/{ref_path}")

    json_content = blob.download_as_text()
    return json.loads(json_content)


def extract_one_polluant(api_key, bucket_name, date_query, polluant_code, polluant_nom):
    """
    Exécute l'extraction complète (demande + téléchargement) pour UN polluant donné.
    Retourne True si succès, False sinon.
    """
    headers = {"apikey": api_key}
    storage_client = storage.Client()

    try:
        export_url = f"{BASE_URL}/MoyH/export?date={date_query}&polluant={polluant_code}"
        print(f"[{polluant_nom}] 1/2 - Demande d'export...")
        resp_export = requests.get(export_url, headers=headers, timeout=30)
        resp_export.raise_for_status()
        file_id = resp_export.text.strip().replace('"', '')
    except Exception as e:
        print(f"❌ [{polluant_nom}] Erreur Étape 1 : {e}")
        return False

    time.sleep(5)
    try:
        download_url = f"{BASE_URL}/download?id={file_id}"
        print(f"[{polluant_nom}] 2/2 - Téléchargement...")
        resp_download = requests.get(download_url, headers=headers, stream=True, timeout=60)

        if resp_download.status_code == 200:
            target_path = f"raw/geodair/{date_query}/MoyH_{polluant_code}.csv"
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(target_path)
            blob.upload_from_string(resp_download.content)
            print(f"✅ [{polluant_nom}] Succès -> gs://{bucket_name}/{target_path}")
            return True
        else:
            print(f"❌ [{polluant_nom}] Erreur Étape 2 : Status {resp_download.status_code}")
            return False
    except Exception as e:
        print(f"❌ [{polluant_nom}] Exception Étape 2 : {e}")
        return False