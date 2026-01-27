#!/usr/bin/env python3
"""
Check the table structure for personalDataCorporate
"""

import psycopg2
from config import Config

config = Config()
pg_conn = psycopg2.connect(
    host=config.database.pg_host,
    port=config.database.pg_port,
    database=config.database.pg_database,
    user=config.database.pg_user,
    password=config.database.pg_password
)

pg_cursor = pg_conn.cursor()
pg_cursor.execute("""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = 'personalDataCorporate' 
    AND column_name NOT IN ('id', 'created_at', 'updated_at')
    ORDER BY ordinal_position
""")

columns = pg_cursor.fetchall()
print(f'Table has {len(columns)} data columns (excluding id, created_at, updated_at)')
for i, (col,) in enumerate(columns, 1):
    print(f'{i:2d}. {col}')

pg_cursor.close()
pg_conn.close()