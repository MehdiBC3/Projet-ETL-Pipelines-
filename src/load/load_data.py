from google.cloud import bigquery
from datetime import datetime, timedelta


def get_yesterday_date_str():
    yesterday = (datetime.now() - timedelta(days=1))
    return yesterday.strftime("%Y-%m-%d")


def load_star_schema_to_bq(bucket_name, date_str, project_id, dataset_id):
    client = bigquery.Client()

    tables_to_load = [
        "DIM_TEMPS",
        "DIM_POLLUANT",
        "DIM_SITE",
        "DIM_QUALITE",
        "FACT_QUALITE_AIR"
    ]

    results = {}
    print(f"Début du chargement CSV pour la date {date_str}")

    for table_name in tables_to_load:
        gcs_uri = f"gs://{bucket_name}/transform/geodair/{date_str}/{table_name}.csv"
        table_ref = f"{project_id}.{dataset_id}.{table_name}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            field_delimiter=";",
            write_disposition="WRITE_APPEND"
        )

        print(f"   -> Chargement de {table_name}")

        try:
            load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
            load_job.result()

            dest_table = client.get_table(table_ref)
            msg = f"OK ({dest_table.num_rows} lignes total)"
            results[table_name] = "Succès"
            print(f"      ✅ {msg}")

        except Exception as e:
            print(f"Erreur: {str(e)}")
            results[table_name] = "Échec"

    return results