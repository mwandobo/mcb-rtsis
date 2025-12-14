#!/usr/bin/env python3
"""
Check all pipeline tables for potential data loss due to primary key constraints
"""

import psycopg2
from config import Config

def check_table_constraints():
    """Check all pipeline tables for primary key constraints and potential data loss"""
    config = Config()
    
    conn = psycopg2.connect(
        host=config.database.pg_host,
        port=config.database.pg_port,
        database=config.database.pg_database,
        user=config.database.pg_user,
        password=config.database.pg_password
    )
    
    cursor = conn.cursor()
    
    # Tables used in our pipeline
    pipeline_tables = {
        'cash_information': 'PRIMARY KEY (branch_code, transaction_date)',
        'asset_owned': 'PRIMARY KEY ("assetType", "acquisitionDate")', 
        'balances_bot': 'PRIMARY KEY ("accountNumber", "transactionDate")',
        'balances_with_mnos': 'PRIMARY KEY ("tillNumber", "mnoCode")',
        'balance_with_other_bank': 'PRIMARY KEY ("accountNumber", "transactionDate")',
        'other_assets': 'NO PRIMARY KEY (fixed)'
    }
    
    print("=== PIPELINE TABLES PRIMARY KEY ANALYSIS ===")
    print()
    
    for table_name, pk_info in pipeline_tables.items():
        try:
            cursor.execute(f'SELECT COUNT(*) FROM {table_name}')
            count = cursor.fetchone()[0]
            
            status = "⚠️  POTENTIAL DATA LOSS" if "PRIMARY KEY" in pk_info else "✅ NO CONSTRAINT"
            
            print(f"📊 {table_name:<25} | {count:>6} records | {status}")
            print(f"   Constraint: {pk_info}")
            print()
            
        except Exception as e:
            print(f"❌ {table_name:<25} | ERROR: {e}")
            print()
    
    print("=== RECOMMENDATION ===")
    print("Consider removing PRIMARY KEY constraints from transactional tables")
    print("to prevent data loss when multiple transactions have the same key combination.")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    check_table_constraints()