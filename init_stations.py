import os
import requests
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def init_ref():
    print("Récupération du référentiel stations...")
    url = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json"
    r = requests.get(url).json()
    
    stations = r['data']['stations']
    data_to_insert = []
    data_to_insert.append({
        "station_id": int(s['station_id']),
        "name": s['name'],
        "lat": lat,
        "lon": lon,
        "capacity": int(s['capacity']),
        })
    
    # Upsert par blocs de 500 pour éviter les erreurs de payload trop lourd
    for i in range(0, len(data_to_insert), 500):
        batch = data_to_insert[i:i+500]
        supabase.table("station_information").upsert(batch).execute()
    print(f"{len(data_to_insert)} stations enregistrées dans le référentiel.")

if __name__ == "__main__":
    init_ref()