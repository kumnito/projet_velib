import os
import requests
import holidays
import time
from supabase import create_client
from datetime import datetime

# Récupération de l'URL de connexion depuis les Secrets GitHub
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_with_retry(url, retries=3, delay=10):
    """Récupère une URL avec plusieurs tentatives"""
    for i in range(retries):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            if i < retries - 1:
                time.sleep(delay)
            else:
                return None

def get_vacation_status():
    """Vérifie les vacances scolaires à Paris (Zone C)."""
    today = datetime.now().strftime("%Y-%m-%d")
    url = f"https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets/fr-en-calendrier-scolaire/records?where=datetime_start%20%3C%3D%20'{today}'%20AND%20datetime_end%20%3E%3D%20'{today}'%20AND%20location%3D'Paris'"
    try:
        r = requests.get(url, timeout=10)
        return r.json().get('total_count', 0) > 0
    except:
        return False

def ingest():
    print("Démarrage de la synchronisation Supabase...")
    fr_holidays = holidays.France()
    now = datetime.now()
    now_iso = now.isoformat()

    # --- COLLECTE DES DONNÉES ---
    # Utilisation des URLs
    r_status = fetch_with_retry("https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json")
    r_weather = fetch_with_retry("https://api.open-meteo.com/v1/forecast?latitude=48.8566&longitude=2.3522&current=apparent_temperature,weather_code")
    
    is_holiday = now in fr_holidays
    is_vacation = get_vacation_status()

    if not r_status:
        print("Impossible de récupérer les données Vélib. Abandon.")
        return

    # --- PRÉPARATION DES DONNÉES ---
    temp = r_weather['current']['apparent_temperature'] if r_weather else 0.0
    w_code = r_weather['current']['weather_code'] if r_weather else 0
    
    records = []
    stations = r_status['data']['stations']

    for s in stations:
        # Extraction des compteurs
        bikes = s.get('num_bikes_available_types', [{}, {}])
        meca = bikes[0].get('mechanical', 0)
        ebike = bikes[1].get('ebike', 0)
        docks = s.get('num_docks_available', 0)
        
        # Calcul du capacity_status
        total_capacity = meca + ebike + docks
        capa_pct = float((meca + ebike) / total_capacity * 100) if total_capacity > 0 else 0.0

        records.append({
            "station_id": int(s['station_id']),
            "bikes_mechanical": int(meca),
            "bikes_ebike": int(ebike),
            "numDocksAvailable": int(docks),
            "is_renting": bool(s.get('is_renting') == 1),
            "capacity_status": capa_pct,
            "datetime": now_iso,
            "is_holiday": bool(is_holiday),
            "is_vacation": bool(is_vacation),
            "apparent_temperature": float(temp),
            "weather_code": int(w_code)
        })

    # --- INSERTION DANS SUPABASE (Incrémentation automatique) ---
    try:
        # On remplace "station_status" par "station_snapshots" comme suggéré par l'erreur
        supabase.table("station_status").insert(records).execute()
        print(f"{len(records)} relevés insérés avec succès.")
    except Exception as e:
        print(f"Erreur lors de l'insertion Supabase: {e}")

if __name__ == "__main__":
    ingest()