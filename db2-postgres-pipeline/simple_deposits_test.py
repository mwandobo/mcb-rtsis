#!/usr/bin/env python3
"""
Simple deposits test
"""

from db2_connection import DB2Connection

def test_simple():
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            # Test very simple query first
            print('Testing very simple query...')
            simple_query = """
            SELECT gte.CUST_ID, gte.DC_AMOUNT
            FROM GLI_TRX_EXTRACT gte
            WHERE gte.JUSTIFIC_DESCR = 'JOURNAL CREDIT'
            FETCH FIRST 5 ROWS ONLY
            """
            cursor.execute(simple_query)
            rows = cursor.fetchall()
            print(f'Simple query returned: {len(rows)} records')
            
            if rows:
                for i, row in enumerate(rows, 1):
                    print(f'  {i}. CUST_ID: {row[0]}, Amount: {row[1]}')
            
    except Exception as e:
        print(f'Error: {e}')
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_simple()