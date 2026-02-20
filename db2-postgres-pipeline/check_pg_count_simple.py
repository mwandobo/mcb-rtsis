#!/usr/bin/env python3
"""
Simple PostgreSQL count check - no RabbitMQ or DB2 imports
"""

import psycopg2
import os

def main():
    try:
        # Direct connection without config module
        conn = psycopg2.connect(
            host=os.getenv('PG_HOST', 'localhost'),
            port=os.getenv('PG_PORT', '5432'),
            database=os.getenv('PG_DATABASE', 'bankdb'),
            user=os.getenv('PG_USER', 'postgres'),
            password=os.getenv('PG_PASSWORD', 'postgres')
        )
        
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM "personalDataInformation"')
        count = cursor.fetchone()[0]
        
        print(f"\n{'='*60}")
        print(f"📊 Personal Data Records in PostgreSQL: {count:,}")
        print(f"   Table: personalDataInformation")
        print(f"{'='*60}\n")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
