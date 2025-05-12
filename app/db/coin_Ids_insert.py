import sqlite3
from app.utils.logger import setup_logger
from app.utils.helper import check_sqlite_connection
from app.data.fetcher import pingCoinGeckoAPI,fetchCoinIds
from dotenv import load_dotenv
import os
from datetime import datetime
import time

# Setup logger

logger = setup_logger()


def init_db(db_name="data/coindb.db"):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS coins (
            id TEXT PRIMARY KEY,
            symbol TEXT,
            name TEXT,
            image TEXT,
            current_price REAL,
            market_cap INTEGER,
            market_cap_rank INTEGER,
            fully_diluted_valuation INTEGER,
            total_volume INTEGER,
            high_24h REAL,
            low_24h REAL,
            price_change_24h REAL,
            price_change_percentage_24h REAL,
            market_cap_change_24h INTEGER,
            market_cap_change_percentage_24h REAL,
            circulating_supply REAL,
            total_supply REAL,
            max_supply REAL,
            ath REAL,
            ath_change_percentage REAL,
            ath_date TEXT,
            atl REAL,
            atl_change_percentage REAL,
            atl_date TEXT,
            roi_times REAL,
            roi_currency TEXT,
            roi_percentage REAL,
            last_updated TEXT
        )
    ''')
    conn.commit()
    conn.close()
    logger.info("Database initialized and table created.")

def add_created_updated_columns(db_name="data/coindb.db"):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Add columns if they don't exist
    cursor.execute("PRAGMA table_info(coins);")
    columns = [col[1] for col in cursor.fetchall()]

    if 'created_date' not in columns:
        cursor.execute("ALTER TABLE coins ADD COLUMN created_date TEXT;")

    if 'updated_date' not in columns:
        cursor.execute("ALTER TABLE coins ADD COLUMN updated_date TEXT;")

    conn.commit()
    conn.close()
    logger.info("Created and updated columns added to the table.")

def insert_data_coinid(data, db_name="data/coindb.db"):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    for coin in data:
        roi = coin.get('roi') or {}
        roi_times = roi.get('times')
        roi_currency = roi.get('currency')
        roi_percentage = roi.get('percentage')

        coin_id = coin.get('id')

        # Check if coin with same id already exists
        cursor.execute("SELECT id FROM coins WHERE id = ?", (coin_id,))
        exists = cursor.fetchone()

        now = datetime.utcnow().isoformat()

        if exists:
            # Update only mutable fields + updated_date
            cursor.execute('''
                UPDATE coins SET
                    current_price = ?, market_cap = ?, market_cap_rank = ?,
                    fully_diluted_valuation = ?, total_volume = ?, high_24h = ?, low_24h = ?,
                    price_change_24h = ?, price_change_percentage_24h = ?,
                    market_cap_change_24h = ?, market_cap_change_percentage_24h = ?,
                    circulating_supply = ?, total_supply = ?, max_supply = ?,
                    ath = ?, ath_change_percentage = ?, ath_date = ?,
                    atl = ?, atl_change_percentage = ?, atl_date = ?,
                    roi_times = ?, roi_currency = ?, roi_percentage = ?, last_updated = ?,
                    updated_date = ?
                WHERE id = ?
            ''', (
                coin.get('current_price'),
                coin.get('market_cap'),
                coin.get('market_cap_rank'),
                coin.get('fully_diluted_valuation'),
                coin.get('total_volume'),
                coin.get('high_24h'),
                coin.get('low_24h'),
                coin.get('price_change_24h'),
                coin.get('price_change_percentage_24h'),
                coin.get('market_cap_change_24h'),
                coin.get('market_cap_change_percentage_24h'),
                coin.get('circulating_supply'),
                coin.get('total_supply'),
                coin.get('max_supply'),
                coin.get('ath'),
                coin.get('ath_change_percentage'),
                coin.get('ath_date'),
                coin.get('atl'),
                coin.get('atl_change_percentage'),
                coin.get('atl_date'),
                roi_times,
                roi_currency,
                roi_percentage,
                coin.get('last_updated'),
                now,
                coin_id
            ))
        else:
            # Insert full row with created_date and updated_date
            cursor.execute('''
                INSERT INTO coins (
                    id, symbol, name, image, current_price, market_cap, market_cap_rank,
                    fully_diluted_valuation, total_volume, high_24h, low_24h, price_change_24h,
                    price_change_percentage_24h, market_cap_change_24h, market_cap_change_percentage_24h,
                    circulating_supply, total_supply, max_supply, ath, ath_change_percentage,
                    ath_date, atl, atl_change_percentage, atl_date,
                    roi_times, roi_currency, roi_percentage, last_updated,
                    created_date, updated_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                coin.get('id'),
                coin.get('symbol'),
                coin.get('name'),
                coin.get('image'),
                coin.get('current_price'),
                coin.get('market_cap'),
                coin.get('market_cap_rank'),
                coin.get('fully_diluted_valuation'),
                coin.get('total_volume'),
                coin.get('high_24h'),
                coin.get('low_24h'),
                coin.get('price_change_24h'),
                coin.get('price_change_percentage_24h'),
                coin.get('market_cap_change_24h'),
                coin.get('market_cap_change_percentage_24h'),
                coin.get('circulating_supply'),
                coin.get('total_supply'),
                coin.get('max_supply'),
                coin.get('ath'),
                coin.get('ath_change_percentage'),
                coin.get('ath_date'),
                coin.get('atl'),
                coin.get('atl_change_percentage'),
                coin.get('atl_date'),
                roi_times,
                roi_currency,
                roi_percentage,
                coin.get('last_updated'),
                now,
                now
            ))

    conn.commit()
    conn.close()
    logger.info(f"{len(data)} coins inserted or updated in database.")


if __name__ == "__main__":
    init_db()
    add_created_updated_columns()

    if pingCoinGeckoAPI():
        for page in range(1, 5):
            data = fetchCoinIds(page=page)
            insert_data_coinid(data)
            print(f"Page {page} processed successfully.")
            time.sleep(1)  # Wait for 5 seconds before processing the next page
    
    else:
        print("APP--Failed: CoinGecko API not reachable")