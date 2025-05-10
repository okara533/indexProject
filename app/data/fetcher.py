from app.utils.logger import setup_logger
import requests
import time
from dotenv import load_dotenv
import os
import json

logger = setup_logger()
load_dotenv()
COIN_GECKO_API_KEY = os.getenv('COIN_GECKO_API_KEY')

def pingCoinGeckoAPI():
    logger.info("Pinging CoinGecko API...")
    # fetch data...
    url = "https://api.coingecko.com/api/v3/ping"

    headers = {
        "accept": "application/json",
        "x-cg-demo-api-key": COIN_GECKO_API_KEY
    }
    try:
        response = requests.get(url, headers=headers)
        responseCode=response.status_code
        if responseCode == 200:
            logger.info("Ping successful, API is up and running.")
            return True
        else:
            logger.error(f"Ping failed with s Status code: {responseCode}")
            return False
    except Exception as e:
        logger.error(f"Error pinging CoinGecko API: {e}")
        return False
def fetchCoinIds(page=1):
    logger.info("Fetching Coin IDs...")
    url = "https://api.coingecko.com/api/v3/coins/markets"
    headers = {
        "accept": "application/json",
        "x-cg-demo-api-key": COIN_GECKO_API_KEY
    }
    params={
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 250,
        "page": page
    }
    try:
        response = requests.get(url, headers=headers, params=params)
        data = response.json()
        coin_ids = [coin['id'] for coin in data]
        logger.info(f"Coin IDs fetched successfully: {len(coin_ids)}")
        return data
    except Exception as e:
        logger.error(f"Error fetching Coin IDs: {e}")
        return []
if __name__ == "__main__":
    if pingCoinGeckoAPI():
        data=fetchCoinIds()
        print(json.dumps(data, indent=4))
    else:
        print("APP--Failed")
