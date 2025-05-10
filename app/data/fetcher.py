from app.utils.logger import setup_logger
import requests
import time
from dotenv import load_dotenv
import os
import json
import pandas as pd
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
    
def histCoinData(coin_id,vs_currency="usd",from_date=None,to_date=None):
    logger.info(f"Fetching historical Coin Data for {coin_id}...")
    params={
        "vs_currency": vs_currency,
        "from": from_date,
        "to": to_date
    }
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart/range"
    headers = {
        "accept": "application/json",
        "x-cg-demo-api-key": COIN_GECKO_API_KEY
    }
    try:
        response = requests.get(url, headers=headers, params=params)
        data = response.json()
        logger.info(f"Historical Coin Data fetched successfully: {len(data)}")

        prices_df = pd.DataFrame(data['prices'], columns=['timestamp', 'price'])
        logger.info(f"Prices DataFrame created: {len(prices_df)}")

        market_caps_df = pd.DataFrame(data['market_caps'], columns=['timestamp', 'market_cap'])
        logger.info(f"Market Caps DataFrame created: {len(market_caps_df)}")

        total_volumes_df = pd.DataFrame(data['total_volumes'], columns=['timestamp', 'total_volume'])
        logger.info(f"Total Volumes DataFrame created: {len(total_volumes_df)}")

        df = prices_df.merge(market_caps_df, on='timestamp').merge(total_volumes_df, on='timestamp')
        logger.info(f"Final DataFrame created: {len(df)}")

        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        logger.info(f"Timestamps converted to datetime: {len(df)}")
        df["id"]=coin_id
        return df
    except Exception as e:
        logger.error(f"Error fetching Historical Coin Data: {e}")
        return []
if __name__ == "__main__":
    if pingCoinGeckoAPI():
        df=histCoinData("bitcoin",vs_currency="usd",from_date="1715367193",to_date="1746903193")
        print(df)
    else:
        print("APP--Failed")
