import httpx
import pandas as pd
import hashlib
import time
import asyncio
from httpx import HTTPStatusError, RequestError
import nest_asyncio
import logging


nest_asyncio.apply()

logging.basicConfig(filename="error_log.txt", level=logging.ERROR, format="%(message)s")

public_key = '7449d7cbc55920fda7c13fbb4210dc51'
private_key = '0433859ac0fc3f1b2a3bf0b8df8c52a1fc0cbc6f'

ts = str(time.time())
hash_value = hashlib.md5(f"{ts}{private_key}{public_key}".encode()).hexdigest()

base_url = "https://gateway.marvel.com/v1/public/characters"

limit = 100  # Limit per request
params = {
    'apikey': public_key,
    'ts': ts,
    'hash': hash_value,
    'limit': limit
}


all_characters = []

async def fetch_characters(offset, max_retries=3):
    """Fetch a batch of characters with the specified offset and retry on failure."""
    retries = 0
    delay = 1  

    while retries < max_retries:
        async with httpx.AsyncClient(timeout=10) as client:
            try:
              
                params['offset'] = offset
                response = await client.get(base_url, params=params)
                response.raise_for_status()  
                data = response.json()
                characters = data['data']['results']
                return [{
                    'id': char['id'],
                    'name': char['name'],
                    'description': char['description'] if char['description'] else "No description available",
                    'comics': char['comics']['available'],
                    'series': char['series']['available'],
                    'stories': char['stories']['available'],
                    'events': char['events']['available']
                } for char in characters]
            except (HTTPStatusError, RequestError) as e:
                
                logging.error(f"Request error for offset {offset} (attempt {retries + 1}/{max_retries}): {e}")
                retries += 1
                await asyncio.sleep(delay)  
                delay *= 2  
    return []  

async def main():
    
    initial_response = await fetch_characters(0)
    if not initial_response:
        print("Initial request failed. Exiting...")
        return
    total_characters = 1500  
    all_characters.extend(initial_response)

    
    offsets = list(range(limit, total_characters, limit))

    
    tasks = [fetch_characters(offset) for offset in offsets]
    results = await asyncio.gather(*tasks)

    
    for result in results:
        all_characters.extend(result)


await main()


if all_characters:
    df = pd.DataFrame(all_characters)
    print("DataFrame created with the following columns:")
    print(df.columns)
    print(df.head())
else:
    print("No data was fetched from the Marvel API.")


if 'df' in locals():
    print("DataFrame contents:")
    print(df)
else:
    print("DataFrame could not be created due to data fetching issues.")
