#!/usr/bin/env python3
"""
Check total deposits records available in DB2
"""

from db2_connection import DB2Connection

def check_total():
    """Check total available deposits records"""
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            # Check total count with the same criteria as pipeline
            query = """
            SELECT COUNT(*) as total_count
            FROM GLI_TRX_EXTRACT gte
                     LEFT JOIN (SELECT *
                                FROM (SELECT wdc.*,
                                             ROW_NUMBER() OVER (PARTITION BY CUST_ID ORDER BY CUSTOMER_BEGIN_DAT DESC) rn
                                      FROM W_DIM_CUSTOMER wdc)
                                WHERE rn = 1) wdc ON wdc.CUST_ID = gte.CUST_ID
                     LEFT JOIN PRODUCT p ON p.ID_PRODUCT = gte.ID_PRODUCT
                     LEFT JOIN (SELECT *
                                FROM (SELECT pa.*,
                                             ROW_NUMBER() OVER (PARTITION BY CUST_ID ORDER BY ACCOUNT_NUMBER) rn
                                      FROM PROFITS_ACCOUNT pa
                                      WHERE PRFT_SYSTEM = 3)
                                WHERE rn = 1) pa ON pa.CUST_ID = gte.CUST_ID
            WHERE gte.JUSTIFIC_DESCR = 'JOURNAL CREDIT'
            """
            
            print("Checking total deposits records in DB2...")
            cursor.execute(query)
            total_count = cursor.fetchone()[0]
            print(f"Total available deposits records: {total_count:,}")
            
            # Also check without ROW_NUMBER to see raw count
            simple_query = """
            SELECT COUNT(*) as total_count
            FROM GLI_TRX_EXTRACT gte
            WHERE gte.JUSTIFIC_DESCR = 'JOURNAL CREDIT'
            """
            
            cursor.execute(simple_query)
            raw_count = cursor.fetchone()[0]
            print(f"Raw count (without ROW_NUMBER): {raw_count:,}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_total()