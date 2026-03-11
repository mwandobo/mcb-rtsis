#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from config import Config

def check_table_schema():
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
        
        # Get table schema
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'balanceWithBot'
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        
        print("balanceWithBot table schema:")
        print("-" * 60)
        for col in columns:
            col_name, data_type, max_length, nullable = col
            length_info = f"({max_length})" if max_length else ""
            print(f"{col_name:<25} {data_type}{length_info:<15} {'NULL' if nullable == 'YES' else 'NOT NULL'}")
        
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_table_schema()