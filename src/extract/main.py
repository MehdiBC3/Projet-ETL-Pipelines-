import functions_framework
import os
import time
from extract_data import get_yesterday_date, extract_one_polluant

BUCKET_RAW_NAME = os.environ.get("raw_bucket_polution_air")
API_KEY = os.environ.get("API_geodair")

POLLUANTS_A_TRAITER = [
    {"code": "03", "nom_court": "NO2", "libelle": "Dioxyde d'azote"},
    {"code": "08", "nom_court": "O3", "libelle": "Ozone"},
    {"code": "24", "nom_court": "PM10", "libelle": "Particules PM10"},
    {"code": "39", "nom_court": "PM2.5", "libelle": "Particules PM2.5"},
    {"code": "01", "nom_court": "SO2", "libelle": "Dioxyde de soufre"},
    {"code": "04", "nom_court": "CO", "libelle": "Monoxyde de carbone"}
]


@functions_framework.http
def run_daily_extraction(request):
    """
    Fonction principale déclenchée par HTTP (Cloud Scheduler).
    Orchestre l'appel aux fonctions d'extraction pour chaque polluant.
    """

    if not BUCKET_RAW_NAME or not API_KEY:
        msg = f"ERREUR FATALE: Il manque une variable ! Bucket='{BUCKET_RAW_NAME}', API_KEY={'OK' if API_KEY else 'MANQUANT'}"
        print(msg)
        return msg, 500

    target_date = get_yesterday_date()
    print(f"Date cible : {target_date}")

    polluants = POLLUANTS_A_TRAITER
    print(f"Référentiel codé en dur : {len(polluants)} polluants à traiter.")

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
        print(f"{status_msg}")
        return status_msg, 206

    return status_msg, 200