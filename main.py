import os
import requests
import holidays
import psycopg2
from datetime import datetime

# Connexion à la base de données Supabase via le Secret GitHub
DB_URL = os.environ.get("SUPABASE_DB_URL")

def run_sync():
    # 1. Récupération des données Métropole (Vélib')
    # Les compteurs comme bikes_mechanical seront traités en 'int'
    response_velib = requests.get("https://velib-metropole-opendata.smovengo.cloud/opendata/GetStationStatus.json")
    data_velib = response_velib.json()['data']['stations']

    # 2. Récupération Météo (Open-Meteo)
    # apparent_temperature est un 'float'
    response_weather = requests.get("https://api.open-meteo.com/v1/forecast?latitude=48.8566&longitude=2.3522&current=apparent_temperature,weather_code")
    data_weather = response_weather.json()['current']

    # 3. Calcul des variables calendaires
    fr_holidays = holidays.France()
    now = datetime.now()
    is_h = now in fr_holidays # Devient un 'bool'

    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    for station in data_velib:
        # On insère uniquement si la station est active
        if station['is_renting'] == 1:
            cur.execute("""
                INSERT INTO station_status (station_id, bikes_mechanical, bikes_ebike, numDocksAvailable, datetime, is_holiday, apparent_temperature, weather_code)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (station_id, datetime) DO NOTHING;
            """, (
                station['station_id'],
                station['num_bikes_available_types'][0]['mechanical'],
                station['num_bikes_available_types'][1]['ebike'],
                station['num_docks_available'],
                now,
                is_h,
                data_weather['apparent_temperature'],
                data_weather['weather_code']
            ))
    
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    run_sync()