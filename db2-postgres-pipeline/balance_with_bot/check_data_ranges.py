#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db2_connection import DB2Connection

def check_data_ranges():
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            # Check the ranges of allowanceProbableLoss and botProvision
            query = """
            SELECT 
                MIN(0) as min_allowance,
                MAX(0) as max_allowance,
                MIN(0) as min_bot_provision,
                MAX(0) as max_bot_provision,
                COUNT(*) as total_records
            FROM GLI_TRX_EXTRACT AS gte
            JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
            JOIN CUSTOMER c ON c.CUST_ID = gte.CUST_ID
            WHERE gl.EXTERNAL_GLACCOUNT = '100028000' and gte.CUST_ID <> 0
            """
            
            cursor.execute(query)
            result = cursor.fetchone()
            
            print("Data ranges from DB2:")
            print("-" * 40)
            print(f"Total records: {result[4]:,}")
            print(f"allowanceProbableLoss: MIN={result[0]}, MAX={result[1]}")
            print(f"botProvision: MIN={result[2]}, MAX={result[3]}")
            
            # Also check some sample values from the actual query
            sample_query = """
            SELECT 
                gte.CUST_ID,
                0 as allowanceProbableLoss,
                0 as botProvision
            FROM GLI_TRX_EXTRACT AS gte
            JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
            JOIN CUSTOMER c ON c.CUST_ID = gte.CUST_ID
            WHERE gl.EXTERNAL_GLACCOUNT = '100028000' and gte.CUST_ID <> 0
            FETCH FIRST 5 ROWS ONLY
            """
            
            cursor.execute(sample_query)
            samples = cursor.fetchall()
            
            print("\nSample values:")
            print("-" * 40)
            for i, sample in enumerate(samples, 1):
                print(f"Record {i}: CUST_ID={sample[0]}, allowanceProbableLoss={sample[1]}, botProvision={sample[2]}")
                
            # Check CUST_ID ranges since that might be the issue
            cust_id_query = """
            SELECT 
                MIN(gte.CUST_ID) as min_cust_id,
                MAX(gte.CUST_ID) as max_cust_id
            FROM GLI_TRX_EXTRACT AS gte
            JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
            JOIN CUSTOMER c ON c.CUST_ID = gte.CUST_ID
            WHERE gl.EXTERNAL_GLACCOUNT = '100028000' and gte.CUST_ID <> 0
            """
            
            cursor.execute(cust_id_query)
            cust_result = cursor.fetchone()
            
            print(f"\nCUST_ID ranges:")
            print(f"MIN CUST_ID: {cust_result[0]}")
            print(f"MAX CUST_ID: {cust_result[1]}")
            
            # Check if CUST_ID is being used in the pipeline
            print(f"\nPostgreSQL integer range: -2,147,483,648 to 2,147,483,647")
            if cust_result[1] > 2147483647:
                print("⚠️  WARNING: MAX CUST_ID exceeds PostgreSQL integer range!")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_data_ranges()