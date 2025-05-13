import sqlite3
import pandas as pd
from app.utils.helper import check_sqlite_connection
from app.utils.logger import setup_logger
logger=setup_logger()

def upsert_daily_rank(db_path: str = 'data/coindb.db'):
    # Connect to your SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    if check_sqlite_connection(db_path):
        try:
            # Make sure the target table exists
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS daily_marketcap_rank (
                    timestamp DATE NOT NULL,
                    id TEXT,
                    rank INTEGER,
                    PRIMARY KEY (timestamp, id)
                )
            ''')

            # Get today's date
            today = pd.Timestamp.today().strftime('%Y-%m-%d')

            # Fetch today's data from the source table
            df = pd.read_sql_query('''
                SELECT timestamp, id, market_cap
                FROM coinGeckoData
                WHERE timestamp = ?
            ''', conn, params=(today,))

            # Check if data exists
            if not df.empty:
                # Rank by market_cap descending
                df['rank'] = df['market_cap'].rank(method='dense', ascending=False).astype(int)
                
                # Select only needed columns
                ranked_df = df[['timestamp', 'id', 'rank']]

                # Insert or replace into the rank table
                ranked_df.to_sql('daily_marketcap_rank', conn, if_exists='append', index=False)

                logger.info(f"Inserted rank data for {today}")
        except Exception as e:
            logger.error(f"Error inserting rank data: {e}")
       
        else:
            logger.info(f"No data found for {today} in crypto_data")

        # Close connection
        conn.commit()
        conn.close()
    else:
        logger.error("Failed to connect to the database")

if __name__ == "__main__":
    upsert_daily_rank()