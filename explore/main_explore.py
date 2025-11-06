import functions_framework
import os
import time
from extract_data_explore import get_yesterday_date, get_pollutants_ref, extract_one_polluant

BUCKET_RAW_NAME = os.environ.get("GCS_BUCKET_RAW")
API_KEY = os.environ.get("GEODAIR_API_KEY")


@functions_framework.http
def run_daily_extraction(request):
    """
    Fonction principale déclenchée par HTTP (Cloud Scheduler).
    Orchestre l'appel aux fonctions d'extraction pour chaque polluant.
    """


    if not BUCKET_RAW_NAME or not API_KEY:
        msg = "ERREUR: Variables d'environnement manquantes (GCS_BUCKET_RAW ou GEODAIR_API_KEY)."
        print(msg)
        return msg, 500

    target_date = get_yesterday_date()


    try:
        polluants = get_pollutants_ref(BUCKET_RAW_NAME)
        print(f"📋 Référentiel chargé : {len(polluants)} polluants à traiter.")
    except Exception as e:
        return f"Erreur chargement référentiel : {e}", 500

    success_count = 0
    errors = []

    for p in polluants:
        code = p.get('code')
        nom = p.get('nom_court', code)

        if extract_one_polluant(API_KEY, BUCKET_RAW_NAME, target_date, code, nom):
            success_count += 1
        else:
            errors.append(nom)

        time.sleep(2)

    status_msg = f"Terminé. Succès : {success_count}/{len(polluants)}."
    if errors:
        status_msg += f" Échecs : {', '.join(errors)}"
        print(f"⚠️ {status_msg}")
        return status_msg, 206  # 206 = Partial Content

    print(f"✅ {status_msg}")
    return status_msg, 200