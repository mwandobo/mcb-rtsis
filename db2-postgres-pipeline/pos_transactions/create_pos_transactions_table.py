#!/usr/bin/env python3
"""
Create the posTransactions table in PostgreSQL for the POS Transactions Streaming Pipeline.
"""

import sys
import os
import psycopg2
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config

def main():
    config = Config()
    conn = psycopg2.connect(
        host=config.database.pg_host,
        port=config.database.pg_port,
        database=config.database.pg_database,
        user=config.database.pg_user,
        password=config.database.pg_password
    )
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS "posTransactions" (
            "reportingDate" VARCHAR(32),
            "posNumber" VARCHAR(32),
            "transactionDate" VARCHAR(32),
            "transactionId" VARCHAR(128) PRIMARY KEY,
            "transactionType" VARCHAR(64),
            "currency" VARCHAR(16),
            "orgCurrencyTransactionAmount" NUMERIC,
            "tzsTransactionAmount" NUMERIC,
            "valueAddedTaxAmount" NUMERIC,
            "exciseDutyAmount" NUMERIC,
            "electronicLevyAmount" NUMERIC
        )
    ''')
    conn.commit()
    print("posTransactions table created or already exists.")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
