#!/usr/bin/env python3
import psycopg2
from config import Config

config = Config()

try:
    conn = psycopg2.connect(
        host=config.database.pg_host,
        port=config.database.pg_port,
        database=config.database.pg_database,
        user=config.database.pg_user,
        password=config.database.pg_password
    )
    
    cursor = conn.cursor()
    
    # Clear all data from the table
    cursor.execute('DELETE FROM "loanInformation"')
    rows_deleted = cursor.rowcount
    
    conn.commit()
    print(f'Cleared {rows_deleted:,} records from loanInformation table')
    
    # Verify table is empty
    cursor.execute('SELECT COUNT(*) FROM "loanInformation"')
    count = cursor.fetchone()[0]
    print(f'Table now contains {count} records')
    
    conn.close()
    
except Exception as e:
    print(f"Error: {e}")