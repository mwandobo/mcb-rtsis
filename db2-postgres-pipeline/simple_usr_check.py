#!/usr/bin/env python3
"""
Simple USR table check
"""

from db2_connection import DB2Connection

def simple_usr_check():
    """Simple USR table check"""
    db2_conn = DB2Connection()
    
    with db2_conn.get_connection() as conn:
        cursor = conn.cursor()
        
        try:
            # Check USR table count
            cursor.execute("SELECT COUNT(*) FROM USR")
            count = cursor.fetchone()[0]
            print(f"USR table records: {count:,}")
            
            # Get structure
            cursor.execute("""
                SELECT COLNAME FROM SYSCAT.COLUMNS 
                WHERE TABNAME = 'USR' 
                ORDER BY COLNO
            """)
            columns = cursor.fetchall()
            print(f"USR columns: {[col[0] for col in columns]}")
            
            # Get sample data
            cursor.execute("SELECT * FROM USR FETCH FIRST 3 ROWS ONLY")
            rows = cursor.fetchall()
            print("Sample USR data:")
            for i, row in enumerate(rows, 1):
                print(f"{i}. {row}")
                
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    simple_usr_check()