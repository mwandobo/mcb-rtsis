#!/usr/bin/env python3
"""
Debug personal data query to see why only 5 records
"""

from db2_connection import DB2Connection

def main():
    print("\n" + "="*60)
    print("🔍 DEBUGGING PERSONAL DATA QUERY")
    print("="*60)
    
    db2_conn = DB2Connection()
    
    # Test different queries to find the issue
    queries = [
        ("Total customers CUST_TYPE='1'", "SELECT COUNT(*) FROM customer WHERE CUST_TYPE = '1'"),
        ("Customers with identification", """
            SELECT COUNT(*) 
            FROM customer c
            LEFT JOIN other_id id ON id.fk_customercust_id = c.cust_id
            LEFT JOIN generic_detail idt ON idt.fk_generic_headpar = id.fkgh_has_type AND idt.serial_num = id.fkgd_has_type
            WHERE c.CUST_TYPE = '1'
            AND UPPER(TRIM(idt.description)) NOT IN ('OTHER TYPE OF IDENTIFICATION', 'BIRTH CERTIFICATE')
        """),
        ("Customers with valid ID (main_flag)", """
            SELECT COUNT(*) 
            FROM customer c
            LEFT JOIN other_id id ON id.fk_customercust_id = c.cust_id 
                AND (CASE WHEN id.serial_no IS NULL THEN '1' ELSE id.main_flag END) = '1'
            LEFT JOIN generic_detail idt ON idt.fk_generic_headpar = id.fkgh_has_type AND idt.serial_num = id.fkgd_has_type
            WHERE c.CUST_TYPE = '1'
            AND UPPER(TRIM(idt.desc