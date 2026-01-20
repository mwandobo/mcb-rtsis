#!/usr/bin/env python3
"""
Test deposits count query
"""

from db2_connection import DB2Connection

def test_count():
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            # Test simple count
            print('Testing simple count...')
            simple_query = "SELECT COUNT(*) FROM GLI_TRX_EXTRACT WHERE JUSTIFIC_DESCR = 'JOURNAL CREDIT'"
            cursor.execute(simple_query)
            count = cursor.fetchone()[0]
            print(f'Total records: {count:,}')
            
            # Test first 10 records
            print('Testing first 10 records...')
            sample_query = """
            SELECT gte.CUST_ID, gte.TRN_DATE, gte.TRN_SNUM, gte.DC_AMOUNT, gte.CURRENCY_SHORT_DES
            FROM GLI_TRX_EXTRACT gte
            WHERE gte.JUSTIFIC_DESCR = 'JOURNAL CREDIT'
            ORDER BY gte.TRN_DATE ASC, gte.TRN_SNUM ASC
            FETCH FIRST 10 ROWS ONLY
            """
            cursor.execute(sample_query)
            rows = cursor.fetchall()
            print(f'Sample records: {len(rows)}')
            for i, row in enumerate(rows[:3], 1):
                print(f'  {i}. CUST_ID: {row[0]}, TRN_DATE: {row[1]}, Amount: {row[3]}, Currency: {row[4]}')
            
    except Exception as e:
        print(f'Error: {e}')
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_count()