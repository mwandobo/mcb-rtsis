#!/usr/bin/env python3
"""
List all tables in PostgreSQL database
"""

import psycopg2
import os

def main():
    try:
        conn = psycopg2.connect(
            host=os.getenv('PG_HOST', 'localhost'),
            port=os.getenv('PG_PORT', '5432'),
            database=os.getenv('PG_DATABASE', 'bankdb'),
            user=os.getenv('PG_USER', 'postgres'),
            password=os.getenv('PG_PASSWORD', 'postgres')
        )
        
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            ORDER BY table_name
        """)
        
        tables = cursor.fetchall()
        
        print(f"\n{'='*60}")
        print(f"📊 Tables in PostgreSQL database 'bankdb':")
        print(f"{'='*60}")
        
        if tables:
            for i, (table_name,) in enumerate(tables, 1):
                print(f"   {i}. {table_name}")
        else:
            print("   No tables found")
        
        print(f"{'='*60}\n")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
