#!/usr/bin/env python3
"""
Clear mobile banking data from PostgreSQL
"""

import psycopg2
from config import Config

def clear_mobile_banking_data():
    """Clear all mobile banking data"""
    
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
        cursor.execute('SELECT COUNT(*) FROM "mobileBanking"')
        before_count = cursor.fetchone()[0]
        print(f"Records before deletion: {before_count}")
        
        # Clear all data
        cursor.execute('DELETE FROM "mobileBanking"')
        conn.commit()
        
        # Get count after deletion
        cursor.execute('SELECT COUNT(*) FROM "mobileBanking"')
        after_count = cursor.fetchone()[0]
        print(f"Records after deletion: {after_count}")
        
        cursor.close()
        conn.close()
        
        print("✅ Mobile banking data cleared successfully")
        
    except Exception as e:
        print(f"❌ Error clearing data: {e}")

if __name__ == "__main__":
    clear_mobile_banking_data()