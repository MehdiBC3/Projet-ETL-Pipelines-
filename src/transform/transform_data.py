import pandas as pd
import numpy as np
from google.cloud import storage
from datetime import datetime, timedelta


def get_yesterday_date_str():
    yesterday = (datetime.now() - timedelta(days=1))
    return yesterday.strftime("%Y-%m-%d")


def list_raw_files(bucket_name, date_str):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    source_prefix = f"raw/geodair/{date_str}/"

    blobs = list(bucket.list_blobs(prefix=source_prefix))

    csv_blobs = [b for b in blobs if b.name.endswith(".csv") and b.size > 0]
    return csv_blobs


def merge_csv_files(blobs, bucket_name):
    all_dfs = []

    for blob in blobs:
        gcs_uri = f"gs://{bucket_name}/{blob.name}"
        try:
            df = pd.read_csv(gcs_uri, sep=';', on_bad_lines='skip', engine='python', dtype={'code_site': str})

            df.columns = df.columns.str.replace('\ufeff', '', regex=True)
            df.columns = df.columns.str.strip()

            all_dfs.append(df)
        except Exception as e:
            print(f"Erreur lecture {gcs_uri}: {e}")

    if not all_dfs:
        return None

    master_df = pd.concat(all_dfs, ignore_index=True)
    return master_df


def determine_danger_level(row):
    try:
        val = float(row['valeur'])
    except (ValueError, TypeError):
        return "Non défini"

    if 'Polluant' not in row or pd.isna(row['Polluant']):
        return "Inconnu"

    pol = str(row['Polluant']).upper()

    seuils = {
        "SO2": [(100, "Bon"), (350, "Moyen"), (500, "Mauvais"), (float('inf'), "Très Mauvais")],
        "NO2": [(50, "Bon"), (100, "Moyen"), (200, "Mauvais"), (float('inf'), "Très Mauvais")],
        "PM10": [(20, "Bon"), (50, "Moyen"), (100, "Mauvais"), (float('inf'), "Très Mauvais")],
        "O3": [(100, "Bon"), (180, "Moyen"), (240, "Mauvais"), (float('inf'), "Très Mauvais")],
    }

    if pol in seuils and pd.notna(val):
        for s, label in seuils[pol]:
            if val <= s:
                return label
    return "Non défini"


def enrich_data_with_kpis(df):
    col_date_debut = [col for col in df.columns if "date" in col.lower() and "début" in col.lower()]

    if not col_date_debut:
        return df

    col_date_ref = col_date_debut[0]
    df[col_date_ref] = pd.to_datetime(df[col_date_ref], errors='coerce')

    if 'valeur' in df.columns and 'Polluant' in df.columns:
        df['Niveau_de_danger'] = df.apply(determine_danger_level, axis=1)

    if "type d'implantation" in df.columns:
        df['Zone'] = df["type d'implantation"].apply(
            lambda x: "Zone urbaine" if "urbain" in str(x).lower()
            else "Zone rurale" if "rural" in str(x).lower()
            else "Autre"
        )

    df['Heure'] = df[col_date_ref].dt.hour
    df['Periode_journee'] = np.where(
        (df['Heure'] >= 6) & (df['Heure'] < 18), 'Jour', 'Nuit'
    )

    df['Date_jour'] = df[col_date_ref].dt.date

    if 'nom site' in df.columns and 'valeur' in df.columns:
        df['Moyenne_journaliere_site'] = df.groupby(['nom site', 'Date_jour'])['valeur'].transform('mean')
        df['Ecart_type_horaire'] = df.groupby(['nom site', col_date_ref])['valeur'].transform('std')

    return df


def generate_star_schema_and_save(df, bucket_name, date_str):
    try:
        col_date_debut = [col for col in df.columns if "date" in col.lower() and "début" in col.lower()][0]
    except IndexError:
        raise ValueError("Colonne Date de début introuvable")

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    def save_part(dataframe, name, folder):
        path = f"{folder}/geodair/{date_str}/{name}.csv"
        blob = bucket.blob(path)
        csv_buffer = dataframe.to_csv(index=False, sep=';').encode('utf-8')
        blob.upload_from_string(csv_buffer, content_type='text/csv')
        print(f"Table {name} sauvegardée (CSV).")
        return path

    dim_temps = df[[col_date_debut, 'Date_jour', 'Heure', 'Periode_journee']].drop_duplicates()
    dim_temps = dim_temps.rename(columns={
        col_date_debut: 'date_debut',
        'Date_jour': 'date_jour',
        'Heure': 'heure'
    })
    dim_temps['jour_semaine'] = dim_temps['date_debut'].dt.day_name()
    dim_temps['mois'] = dim_temps['date_debut'].dt.month
    dim_temps['annee'] = dim_temps['date_debut'].dt.year

    col_unite = None
    possibles = ['unite', 'unité', 'Unité', 'unite_mesure', 'Unité de mesure']
    for c in df.columns:
        if c in possibles:
            col_unite = c
            break

    cols_polluant = ['Polluant']
    if col_unite:
        cols_polluant.append(col_unite)

    dim_polluant = df[cols_polluant].drop_duplicates(subset=['Polluant'])
    dim_polluant = dim_polluant.rename(columns={'Polluant': 'code_polluant'})

    if col_unite:
        dim_polluant = dim_polluant.rename(columns={col_unite: 'unite_mesure'})
    else:
        dim_polluant['unite_mesure'] = 'µg/m3'

    dim_polluant['nom_polluant'] = dim_polluant['code_polluant']

    if 'code site' in df.columns:
        col_code_site = 'code site'
        col_nom_site = 'nom site'
        col_implant = "type d'implantation"
    else:
        col_code_site = 'code_site'
        col_nom_site = 'nom_site'
        col_implant = "type_implant"

    cols_site = [col_code_site, col_nom_site, col_implant]
    if 'latitude' in df.columns and 'longitude' in df.columns:
        cols_site.extend(['latitude', 'longitude'])

    cols_site = [c for c in cols_site if c in df.columns]

    dim_site = df[cols_site].drop_duplicates(subset=[col_code_site])
    dim_site = dim_site.rename(columns={
        col_code_site: 'code_site',
        col_nom_site: 'nom_site',
        col_implant: 'type_implant'
    })

    if 'Niveau_de_danger' in df.columns:
        dim_qualite = df[['Niveau_de_danger']].drop_duplicates()
        dim_qualite = dim_qualite.rename(columns={'Niveau_de_danger': 'signification'})
        dim_qualite['code_qualite'] = dim_qualite['signification'].str.upper().str.replace(' ', '_')
    else:
        dim_qualite = pd.DataFrame(columns=['code_qualite', 'signification'])

    cols_fact = [col_date_debut, 'Polluant', 'valeur']
    if col_code_site in df.columns: cols_fact.append(col_code_site)
    if 'Niveau_de_danger' in df.columns: cols_fact.append('Niveau_de_danger')
    if 'Moyenne_journaliere_site' in df.columns: cols_fact.append('Moyenne_journaliere_site')

    fact_table = df[cols_fact].copy()

    rename_map = {
        col_date_debut: 'date_debut',
        'Polluant': 'code_polluant',
        'valeur': 'valeur',
        col_code_site: 'code_site',
        'Niveau_de_danger': 'code_qualite',
        'Moyenne_journaliere_site': 'valeur_brute'
    }
    fact_table = fact_table.rename(columns=rename_map)

    if 'code_qualite' in fact_table.columns:
        fact_table['code_qualite'] = fact_table['code_qualite'].str.upper().str.replace(' ', '_')

    fact_table['validite'] = fact_table['valeur'].notna()

    files_created = {}
    folder = 'transform'

    files_created['DIM_TEMPS'] = save_part(dim_temps, 'DIM_TEMPS', folder)
    files_created['DIM_POLLUANT'] = save_part(dim_polluant, 'DIM_POLLUANT', folder)
    files_created['DIM_SITE'] = save_part(dim_site, 'DIM_SITE', folder)
    files_created['DIM_QUALITE'] = save_part(dim_qualite, 'DIM_QUALITE', folder)
    files_created['FACT_QUALITE_AIR'] = save_part(fact_table, 'FACT_QUALITE_AIR', folder)

    return files_created