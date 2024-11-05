import httpx
import pandas as pd
import hashlib
import time


public_key = '7449d7cbc55920fda7c13fbb4210dc51'
private_key = '0433859ac0fc3f1b2a3bf0b8df8c52a1fc0cbc6f'

ts = str(time.time())
hash_value = hashlib.md5(f"{ts}{private_key}{public_key}".encode()).hexdigest()

print("Timestamp:", ts)
print("Generated Hash:", hash_value)

base_url = "https://gateway.marvel.com/v1/public/characters"
params = {
    'apikey': public_key,
    'ts': ts,
    'hash': hash_value,
    'limit': 100 
}

print("Sending request to Marvel API with httpx...")
try:
    response = httpx.get(base_url, params=params, verify=False, timeout=10)
    
    print("Status Code:", response.status_code)
    if response.status_code == 200:
        data = response.json()
        characters = data['data']['results']
        print(f"Number of characters fetched: {len(characters)}")

        df = pd.DataFrame([{
            'id': char['id'],
            'name': char['name'],
            'description': char['description'],
            'modified': char['modified']
        } for char in characters])

        print("DataFrame created with the following columns:")
        print(df.columns)
        print(df.head()) 

    else:
        print("Error en la solicitud:", response.status_code)

except httpx.HTTPStatusError as err:
    print("HTTP Error:", err)
except httpx.RequestError as e:
    print("Request Error:", e)

if 'df' in locals():
    print("DataFrame contents:")
    print(df)
else:
    print("DataFrame could not be created due to data fetching issues.")
