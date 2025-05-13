import sqlite3
import pandas as pd
from app.utils.helper import check_sqlite_connection
from app.utils.logger import setup_logger
logger=setup_logger()

def upsert_historical_rank(db_path: str = 'data/coindb.db'):
    # Connect to your SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    if check_sqlite_connection(db_path):
        # (Re)create the target rank table
        cursor.execute('DROP TABLE IF EXISTS daily_marketcap_rank')
        cursor.execute('''
            CREATE TABLE daily_marketcap_rank (
                timestamp DATE NOT NULL,
                id TEXT,
                rank INTEGER,
                PRIMARY KEY (timestamp, id)
            )
        ''')

        # Load all data from the source table
        df = pd.read_sql_query('SELECT timestamp, id, market_cap FROM coinGeckoData', conn)

        # Ensure proper date format (in case it's not)
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.strftime('%Y-%m-%d')

        # Group by date and rank by market cap descending
        ranked_rows = []

        for timestamp, group in df.groupby('timestamp'):
            group = group.copy()
            group['rank'] = group['market_cap'].rank(method='dense', ascending=False).astype(int)
            ranked_rows.append(group[['timestamp', 'id', 'rank']])
            logger.info(f"Inserted index data for date: {timestamp}")
        # Concatenate all ranked data
        ranked_df = pd.concat(ranked_rows, ignore_index=True)

        # Insert into the database
        ranked_df.to_sql('daily_marketcap_rank', conn, if_exists='append', index=False)

        logger.info("✅ Historical ranking completed.")

        # Close connection
        conn.commit()
        conn.close()
    else:
        logger.error(f"Failed to connect to SQLite database: {db_path}")

if __name__ == "__main__":
    logger.info("Starting historical ranking script...")
    upsert_historical_rank()
    logger.info("Historical rank script executed successfully.")