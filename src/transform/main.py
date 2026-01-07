import functions_framework
import os
from transform_data import get_yesterday_date_str, list_raw_files, merge_csv_files, enrich_data_with_kpis, \
    generate_star_schema_and_save

BUCKET_NAME = os.environ.get("GSC_BUCKET_NAME")


@functions_framework.http
def run_daily_transform(request):
    print("Démarrage du job de transformation (T) quotidien.")

    if not BUCKET_NAME:
        msg = "ERREUR FATALE: Variable d'environnement GCS_BUCKET_NAME manquante."
        print(msg)
        return msg, 500

    date_str = get_yesterday_date_str()

    raw_files = list_raw_files(BUCKET_NAME, date_str)
    if not raw_files:
        msg = f"Rien à transformer : Aucun fichier trouvé pour le {date_str}."
        print(msg)
        return msg, 200

    master_df = merge_csv_files(raw_files, BUCKET_NAME)

    if master_df is None or master_df.empty:
        msg = "Échec de la fusion des fichiers bruts ou DataFrame vide."
        print(msg)
        return msg, 500

    master_df = enrich_data_with_kpis(master_df)

    try:
        files_created = generate_star_schema_and_save(master_df, BUCKET_NAME, date_str)

        msg = f"Transformation terminée. {len(files_created)} tables créées."
        print(msg)
        return msg, 200

    except Exception as e:
        msg = f"Échec de la génération du Star Schema: {e}"
        print(msg)
        return msg, 500