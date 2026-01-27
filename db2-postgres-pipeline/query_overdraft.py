#!/usr/bin/env python3
"""
Query Overdraft Data
"""

import psycopg2
from config import Config

def query_overdraft_data():
    """Query overdraft data from PostgreSQL"""
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
        
        print("Overdraft Data Query")
        print("=" * 50)
        
        # Total count
        cursor.execute("SELECT COUNT(*) FROM overdraft")
        total_count = cursor.fetchone()[0]
        print(f"Total overdraft records: {total_count:,}")
        
        if total_count > 0:
            # Currency breakdown
            cursor.execute("""
                SELECT currency, COUNT(*) as count, 
                       SUM(orgOutstandingAmount) as total_outstanding
                FROM overdraft 
                WHERE currency IS NOT NULL 
                GROUP BY currency 
                ORDER BY count DESC
            """)
            
            print(f"\nCurrency breakdown:")
            for row in cursor.fetchall():
                currency, count, outstanding = row
                outstanding_str = f"{outstanding:,.2f}" if outstanding else "0.00"
                print(f"  {currency}: {count:,} records, Outstanding: {outstanding_str}")
            
            # Loan product types
            cursor.execute("""
                SELECT loanProductType, COUNT(*) as count
                FROM overdraft 
                WHERE loanProductType IS NOT NULL 
                GROUP BY loanProductType 
                ORDER BY count DESC
                LIMIT 10
            """)
            
            print(f"\nTop loan product types:")
            for row in cursor.fetchall():
                product_type, count = row
                print(f"  {product_type}: {count:,} records")
            
            # Recent records
            cursor.execute("""
                SELECT accountNumber, clientName, currency, orgOutstandingAmount, contractDate
                FROM overdraft 
                ORDER BY created_at DESC 
                LIMIT 5
            """)
            
            print(f"\nRecent records:")
            for i, row in enumerate(cursor.fetchall(), 1):
                account, client, currency, outstanding, contract_date = row
                outstanding_str = f"{outstanding:,.2f}" if outstanding else "0.00"
                print(f"  {i}. Account: {account}, Client: {client[:30]}{'...' if len(client) > 30 else ''}")
                print(f"     Currency: {currency}, Outstanding: {outstanding_str}, Contract: {contract_date}")
        
        conn.close()
        
    except Exception as e:
        print(f"Error querying overdraft data: {e}")

if __name__ == "__main__":
    query_overdraft_data()