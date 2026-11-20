#!/usr/bin/env python3
import sys
sys.path.append('db2-postgres-pipeline')
from db2_connection import DB2Connection

def query_profits_tables():
    db2_conn = DB2Connection()
    with db2_conn.get_connection() as conn:
        cursor = conn.cursor()
        
        # Get all tables in PROFITS schema
        cursor.execute("""
        SELECT TABNAME, REMARKS 
        FROM SYSCAT.TABLES 
        WHERE TABSCHEMA = 'PROFITS' 
        ORDER BY TABNAME
        """)
        
        tables = cursor.fetchall()
        print('PROFITS Database Tables:')
        print('=' * 80)
        for table, remarks in tables:
            print(f'{table:<35} {remarks or "":<45}')
        
        print(f'\nTotal tables: {len(tables)}')

if __name__ == "__main__":
    query_profits_tables()