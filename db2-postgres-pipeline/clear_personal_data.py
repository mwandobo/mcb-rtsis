#!/usr/bin/env python3
"""
Clear personal data from PostgreSQL
"""

import psycopg2
from config import Config

def clear_personal_data():
    """Clear all personal data records from PostgreSQL"""
    
    config = Config()
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        
        cursor = conn.cursor()
        
        # Get count before deletion
        cursor.execute('SELECT COUNT(*) FROM "personalData"')
        count_before = cursor.fetchone()[0]
        
        print(f"Records before deletion: {count_before}")
        
        # Clear all records
        cursor.execute('DELETE FROM "personalData"')
        
        # Get count after deletion
        cursor.execute('SELECT COUNT(*) FROM "personalData"')
        count_after = cursor.fetchone()[0]
        
        print(f"Records after deletion: {count_after}")
        
        # Commit the changes
        conn.commit()
        cursor.close()
        conn.close()
        
        print("✅ Personal data cleared successfully")
        
    except Exception as e:
        print(f"❌ Error clearing personal data: {e}")
        raise

if __name__ == "__main__":
    clear_personal_data()