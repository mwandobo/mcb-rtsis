#!/usr/bin/env python3
"""
Check simple deposits count in DB2
"""

from db2_connection import DB2Connection

def check_simple():
    """Check simple deposits count"""
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            # Simple count without joins
            query = """
            SELECT COUNT(*) as total_count
            FROM GLI_TRX_EXTRACT gte
            WHERE gte.JUSTIFIC_DESCR = 'JOURNAL CREDIT'
            """
            
            print("Checking simple deposits count...")
            cursor.execute(query)
            total_count = cursor.fetchone()[0]
            print(f"Total records with JUSTIFIC_DESCR = 'JOURNAL CREDIT': {total_count:,}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_simple()