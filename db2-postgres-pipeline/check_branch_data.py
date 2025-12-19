#!/usr/bin/env python3
"""
Check Branch Data in PostgreSQL
"""

import psycopg2

def check_branch_data():
    """Check what branch data is in PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host='localhost',
            database='bankdb',
            user='postgres',
            password='postgres'
        )
        cursor = conn.cursor()
        
        # Get count
        cursor.execute('SELECT COUNT(*) FROM branch')
        count = cursor.fetchone()[0]
        print(f'Total branch records in PostgreSQL: {count}')
        
        # Get branch details
        cursor.execute('SELECT "branchCode", "branchName", "branchStatus" FROM branch ORDER BY "branchCode"')
        rows = cursor.fetchall()
        
        print('\nBranch records:')
        for row in rows:
            print(f'  Branch {row[0]}: {row[1]} - {row[2]}')
        
        conn.close()
        
    except Exception as e:
        print(f'Error: {e}')

if __name__ == "__main__":
    check_branch_data()