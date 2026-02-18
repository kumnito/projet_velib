import os
import requests
import holidays
import psycopg2
from datetime import datetime

# Récupération de l'URL de connexion depuis les Secrets GitHub
DB_URL = os.environ.get("SUPABASE_DB_URL")

def is_paris_vacation(date_now):
    """Vérifie si la date est dans les vacances scolaires (Zone C - Paris)"""
    try:
        # API Éducation Nationale pour les vacances
        year = date_now.year
        url = f"https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets/fr-en-calendrier-scolaire/records?where=location%3D'Paris'%20AND%20annee_scolaire%3D'{year}-{year+1}'"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            for record in data.get('results', []):
                start = datetime.fromisoformat(record['start_date'].replace('Z', '+00:00')).replace(tzinfo=None)
                end = datetime.fromisoformat(record['end_date'].replace('Z', '+00:00')).replace(tzinfo=None)
                if start <= date_now <= end:
                    return True
        return False
    except Exception as e:
        print(f"Erreur API Vacances : {e}")
        return False

def run_sync():
    # 1. Configuration des APIs
    VELIB_API_URL = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json"
    WEATHER_API_URL = "https://api.open-meteo.com/v1/forecast?latitude=48.8566&longitude=2.3522&current=apparent_temperature,weather_code"

    # 2. Récupération des données avec gestion d'erreurs (Objectif 0% NA)
    try:
        # Données Vélib'
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        res_velib = requests.get(VELIB_API_URL, headers=headers, timeout=20)
        res_velib.raise_for_status()
        stations = res_velib.json()['data']['stations']

        # Données Météo
        res_weather = requests.get(WEATHER_API_URL, timeout=10)
        res_weather.raise_for_status()
        current_w = res_weather.json()['current']
        temp = float(current_w['apparent_temperature'])
        w_code = int(current_w['weather_code'])

        # Variables temporelles et calendaires
        now = datetime.now()
        is_h = bool(now in holidays.France())
        is_v = is_paris_vacation(now)

        # 3. Connexion Supabase
        conn = psycopg2.connect(DB_URL)
        cur = conn.cursor()

        for station in stations:
            s_id = int(station['station_id'])
            meca = int(station['num_bikes_available_types'][0]['mechanical'])
            ebike = int(station['num_bikes_available_types'][1]['ebike'])
            docks = int(station['num_docks_available'])
            renting = bool(station['is_renting'] == 1)

            # Calcul du capacity_status
            total_bikes = meca + ebike
            total_capa = total_bikes + docks
            capa_pct = float((total_bikes / total_capa) * 100) if total_capa > 0 else 0.0

            cur.execute("""
                INSERT INTO station_status (
                    station_id, bikes_mechanical, bikes_ebike, numDocksAvailable, 
                    is_renting, capacity_status, datetime, is_holiday, 
                    is_vacation, apparent_temperature, weather_code
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (station_id, datetime) DO NOTHING;
            """, (s_id, meca, ebike, docks, renting, capa_pct, now, is_h, is_v, temp, w_code))
        
        conn.commit()
        print(f"Sync terminée : {len(stations)} stations à {now} (Vacances: {is_v})")
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Erreur critique lors de la sync : {e}")

if __name__ == "__main__":
    run_sync()