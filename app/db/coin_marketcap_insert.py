import sqlite3
import pandas as pd
from app.utils.logger import setup_logger
from app.utils.helper import check_sqlite_connection

from app.data.fetcher import pingCoinGeckoAPI,fetchCoinIds,histCoinData
import time

logger = setup_logger()

def upsert_crypto_data(df: pd.DataFrame, db_path: str = 'data/coindb.db', table_name: str = 'coinGeckoData'):
    # Ensure timestamp column is datetime.date type
    df = df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp']).dt.date
    df=df[["timestamp", "id", "price", "market_cap", "total_volume"]]
    # Connect to sqlite3 database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    if check_sqlite_connection(db_path):
        logger.info(f"Connected to SQLite database: {db_path}")
        # Create table if not exists (with created and updated columns)
        create_table_query = f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            timestamp DATE NOT NULL,
            id TEXT NOT NULL,
            price REAL,
            market_cap REAL,
            total_volume REAL,
            created DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id, timestamp)
        );
        '''
        cursor.execute(create_table_query)
        # Prepare the upsert SQL with created/updated handling
        upsert_query = f'''
        INSERT INTO {table_name} (timestamp, id, price, market_cap, total_volume)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(id, timestamp) DO UPDATE SET
            price = excluded.price,
            market_cap = excluded.market_cap,
            total_volume = excluded.total_volume,
            updated = CURRENT_TIMESTAMP;
        '''
        conn.commit()

        logger.info(f"Connected to SQLite database: {db_path}")
        # Execute for each row
        data_tuples = list(df.itertuples(index=False, name=None))
        cursor.executemany(upsert_query, data_tuples)
        logger.info(f"Data inserted into SQLite database: {len(df)}")
        # Commit and close connection
        conn.commit()
        conn.close()

        print(f"Upserted {len(df)} rows into '{table_name}' table.")
    else:
        logger.error("Failed to connect to SQLite database")
def getCoinId(db_path: str = 'data/coindb.db', table_name: str = 'coins'):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    if check_sqlite_connection(db_path, table_name):
        logger.info("Connected to SQLite database: {db_path}")
        cursor.execute(f"""
                        select id,market_cap_rank  from {table_name}
                        order by market_cap_rank asc
                        """)
        coin_ids = cursor.fetchall()
        conn.close()

        return [row[0] for row in coin_ids]
    else:
        return []
if __name__ == "__main__":


    if pingCoinGeckoAPI():
        #get coids
        coin_ids = getCoinId()
        logger.info(f"Found {len(coin_ids)} coins in database.")
        count = 1
        for id in coin_ids[:600]:
            print(count)
            logger.info(f"Processing coin ID: {id}")
            data = histCoinData(id,vs_currency="usd",from_date="1715529885",to_date="1747065885")
            upsert_crypto_data(data)
            #print(f"Page {page} processed successfully.")
            time.sleep(2)  # Wait for 5 seconds before processing the next page
            count += 1
    else:
        print("APP--Failed: CoinGecko API not reachable")