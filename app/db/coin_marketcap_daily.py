import sqlite3
import pandas as pd
from app.utils.logger import setup_logger
from app.utils.helper import check_sqlite_connection

from app.data.fetcher import pingCoinGeckoAPI,fetchCoinIds
import time
from datetime import date

logger = setup_logger()

def upsert_crypto_data_daily(df: pd.DataFrame, db_path: str = 'data/coindb.db', table_name: str = 'coinGeckoData'):
    # Ensure timestamp column is datetime.date type
    df = df.copy()
    df['timestamp'] = date.today().strftime("%Y-%m-%d")
    #turn daily data to coingecko Data table only this secitone updated upsert_crypto_data
    df=df[["timestamp","id","current_price","market_cap","total_volume"]]
    df.columns=["date","id","price","market_cap","total_volume"]
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

if __name__ == "__main__":
    if pingCoinGeckoAPI():
        #get coids
        for page in range(1,5):       
            logger.info(f"Processing coin ID: {id}")
            data = fetchCoinIds(page)
            df=pd.DataFrame(data)
            upsert_crypto_data_daily(df)
            #print(f"Page {page} processed successfully.")
            time.sleep(2)  # Wait for 5 seconds before processing the next page
    else:
        print("APP--Failed: CoinGecko API not reachable")