from datetime import datetime,timedelta
def checkResponse(response):
    if response.status_code == 200:
        return True
    else:
        return False
    
import sqlite3

def check_sqlite_connection(db_path: str, required_table: str="coins") -> bool:
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check if required_table exists
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (required_table,))
        result = cursor.fetchone()
        
        conn.close()
        
        # Table exists → return True
        return result is not None

    except sqlite3.Error:
        return False
    
def get_today_and_1year_ago_timestamps():
    # Get current timestamp and one year ago's timestamp
    today = datetime.today()
    one_year_ago = today - timedelta(days=365)
    today_timestamp = int(today.timestamp())
    one_year_ago_timestamp = int(one_year_ago.timestamp())

    return today_timestamp, one_year_ago_timestamp