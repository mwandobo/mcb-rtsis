#!/usr/bin/env python3
"""
Query ICBM Transaction Data
"""

import psycopg2
from config import Config

def query_icbm_transaction_data():
    """Query ICBM transaction data from PostgreSQL"""
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
        
        print("ICBM Transaction Data Query")
        print("=" * 55)
        
        # Total count
        cursor.execute('SELECT COUNT(*) FROM "icbmTransactions"')
        total_count = cursor.fetchone()[0]
        print(f"Total ICBM transaction records: {total_count:,}")
        
        if total_count > 0:
            # Transaction type breakdown
            cursor.execute("""
                SELECT "transactionType", COUNT(*) as count, 
                       SUM("tzsAmount") as total_amount,
                       AVG("tzsAmount") as avg_amount
                FROM "icbmTransactions" 
                WHERE "transactionType" IS NOT NULL 
                GROUP BY "transactionType" 
                ORDER BY count DESC
            """)
            
            print(f"\nTransaction type breakdown:")
            for row in cursor.fetchall():
                trans_type, count, total_amount, avg_amount = row
                total_str = f"{total_amount:,.2f}" if total_amount else "0.00"
                avg_str = f"{avg_amount:,.2f}" if avg_amount else "0.00"
                print(f"  {trans_type}: {count:,} records, Total: {total_str} TZS, Avg: {avg_str} TZS")
            
            # Date range
            cursor.execute("""
                SELECT MIN("transactionDate") as earliest, MAX("transactionDate") as latest
                FROM "icbmTransactions" 
                WHERE "transactionDate" IS NOT NULL
            """)
            
            earliest, latest = cursor.fetchone()
            print(f"\nDate range: {earliest} to {latest}")
            
            # Top lenders
            cursor.execute("""
                SELECT "lenderName", COUNT(*) as count, SUM("tzsAmount") as total_amount
                FROM "icbmTransactions" 
                WHERE "lenderName" IS NOT NULL 
                GROUP BY "lenderName" 
                ORDER BY count DESC
                LIMIT 5
            """)
            
            print(f"\nTop lenders:")
            for row in cursor.fetchall():
                lender, count, total_amount = row
                total_str = f"{total_amount:,.2f}" if total_amount else "0.00"
                print(f"  {lender}: {count:,} transactions, Total: {total_str} TZS")
            
            # Interest rate statistics
            cursor.execute("""
                SELECT MIN("interestRate") as min_rate, 
                       MAX("interestRate") as max_rate,
                       AVG("interestRate") as avg_rate
                FROM "icbmTransactions" 
                WHERE "interestRate" IS NOT NULL
            """)
            
            min_rate, max_rate, avg_rate = cursor.fetchone()
            if min_rate is not None:
                print(f"\nInterest rate statistics:")
                print(f"  Min: {min_rate:.4f}%, Max: {max_rate:.4f}%, Avg: {avg_rate:.4f}%")
            
            # Recent records
            cursor.execute("""
                SELECT "transactionDate", "lenderName", "borrowerName", "transactionType", 
                       "tzsAmount", tenure, "interestRate"
                FROM "icbmTransactions" 
                ORDER BY created_at DESC 
                LIMIT 5
            """)
            
            print(f"\nRecent records:")
            for i, row in enumerate(cursor.fetchall(), 1):
                trans_date, lender, borrower, trans_type, amount, tenure, rate = row
                amount_str = f"{amount:,.2f}" if amount else "0.00"
                rate_str = f"{rate:.4f}%" if rate else "N/A"
                print(f"  {i}. Date: {trans_date}, Type: {trans_type}")
                print(f"     Lender: {lender}, Borrower: {borrower}")
                print(f"     Amount: {amount_str} TZS, Tenure: {tenure} days, Rate: {rate_str}")
        
        conn.close()
        
    except Exception as e:
        print(f"Error querying ICBM transaction data: {e}")

if __name__ == "__main__":
    query_icbm_transaction_data()