import os
import requests
from supabase import create_client

# INITIALISATION
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def init_ref():
    print("Récupération du référentiel stations...")
    url = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        stations = data['data']['stations']
        
        data_to_insert = []
        for s in stations:
            data_to_insert.append({
                "station_id": int(s['station_id']),
                "name": s['name'],
                "lat": float(s['lat']),
                "lon": float(s['lon']),
                "capacity": int(s['capacity'])
            })
        
        # Envoi vers la table station_information
        supabase.table("station_information").upsert(data_to_insert).execute()
        print(f"{len(data_to_insert)} stations synchronisées.")
        
    except Exception as e:
        print(f"Erreur : {e}")

if __name__ == "__main__":
    init_ref()