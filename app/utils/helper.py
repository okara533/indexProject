def checkResponse(response):
    if response.status_code == 200:
        return True
    else:
        return False
    
import sqlite3

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
