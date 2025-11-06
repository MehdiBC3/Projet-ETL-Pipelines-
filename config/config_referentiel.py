from google.cloud import storage
import json

# Configuration
BUCKET_NAME = "raw_bucket_polution_air"
REF_PATH = "referentiel/polluants.json"
PROJECT_ID = "pipelinequaliteair "

def initialize_pollutant_ref():
    # Liste issue de la documentation officielle Geod'air
    polluants = [
        {"code": "03", "nom_court": "NO2", "libelle": "Dioxyde d'azote"},
        {"code": "08", "nom_court": "O3", "libelle": "Ozone"},
        {"code": "24", "nom_court": "PM10", "libelle": "Particules PM10"},
        {"code": "39", "nom_court": "PM2.5", "libelle": "Particules PM2.5"},
        {"code": "01", "nom_court": "SO2", "libelle": "Dioxyde de soufre"},
        {"code": "04", "nom_court": "CO", "libelle": "Monoxyde de carbone"}
    ]

    client = storage.Client(PROJECT_ID  )
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(REF_PATH)

    # Sauvegarde en format JSON (plus facile à lire par le code ensuite)
    blob.upload_from_string(
        json.dumps(polluants, indent=2, ensure_ascii=False),
        content_type='application/json'
    )
    print(f"Succès : Référentiel créé sur gs://{BUCKET_NAME}/{REF_PATH}")


if __name__ == "__main__":
    initialize_pollutant_ref()