import functions_framework
import os
from load_data import get_yesterday_date_str, load_star_schema_to_bq

BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
DATASET_ID = "geodair_prod"


@functions_framework.http
def run_daily_load(request):
    """
    Fonction (L) de l'ETL déclenchée par HTTP.
    Charge le schema en étoile dans BigQuery.
    """
    print("Démarrage du job de chargement (L).")

    msg = "Initialisation..."

    if not BUCKET_NAME or not PROJECT_ID:
        msg = "ERREUR FATALE: Variables d'environnement manquantes."
        print(msg)
        return msg, 500

    try:
        date_str = get_yesterday_date_str()

        results = load_star_schema_to_bq(BUCKET_NAME, date_str, PROJECT_ID, DATASET_ID)

        failures = [table for table, status in results.items() if status == "Échec"]

        if failures:
            msg = f"Chargement partiel. Échecs sur : {', '.join(failures)}"
            print(msg)
            return msg, 500
        else:
            msg = f"Chargement complet terminé avec succès pour le {date_str}."
            print(msg)
            return msg, 200

    except Exception as e:
        msg = f"Crash critique du script : {str(e)}"
        print(msg)
        return msg, 500