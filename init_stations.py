import os
import requests
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def init_ref():
    print("📥 Récupération du référentiel stations...")
    url = "https://velib-metropole-opendata.smovengo.cloud/opendata/GetStationInformation.json"
    r = requests.get(url).json()
    
    stations = r['data']['stations']
    data_to_insert = []
    
    for s in stations:
        data_to_insert.append({
            "station_id": int(s['station_id']),
            "name": s['name'],
            "lat": float(s['lat']),
            "lon": float(s['lon']),
            "capacity": int(s['capacity']),
            "nom_commune": s.get('nom_commune', 'Paris') # Parfois absent selon l'API
        })
    
    # Upsert permet de remplir ou de mettre à jour si ça existe déjà
    supabase.table("station_information").upsert(data_to_insert).execute()
    print(f"✅ {len(data_to_insert)} stations enregistrées dans le référentiel.")

if __name__ == "__main__":
    init_ref()