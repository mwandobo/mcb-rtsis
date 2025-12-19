#!/usr/bin/env python3
"""
Script to test the branch table structure and insert sample data
"""

import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime, date

# Load environment variables
load_dotenv()

def test_branch_table():
    """Test the branch table by inserting sample data"""
    
    # Database connection parameters
    conn_params = {
        'host': os.getenv('PG_HOST', 'localhost'),
        'port': int(os.getenv('PG_PORT', '5432')),
        'database': os.getenv('PG_DATABASE', 'bank_data'),
        'user': os.getenv('PG_USER', 'postgres'),
        'password': os.getenv('PG_PASSWORD', 'postgres')
    }
    
    try:
        # Connect to PostgreSQL
        print(f"Connecting to PostgreSQL at {conn_params['host']}:{conn_params['port']}")
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        # Check if branch table exists and get its structure
        cursor.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = 'branch'
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        print(f"📋 Branch table structure ({len(columns)} columns):")
        for col_name, data_type, nullable, default in columns:
            nullable_str = "NULL" if nullable == "YES" else "NOT NULL"
            default_str = f" DEFAULT {default}" if default else ""
            print(f"  - {col_name}: {data_type} {nullable_str}{default_str}")
        
        # Insert sample branch data
        sample_data = [
            {
                'reportingDate': datetime.now(),
                'branchName': 'Head Office',
                'taxIdentificationNumber': 'TIN123456789',
                'businessLicense': 'BL000001',
                'branchCode': '001',
                'qrFsrCode': 'QR00000001',
                'region': 'Dar es Salaam',
                'district': 'Ilala',
                'ward': 'Kivukoni',
                'street': 'Sokoine Drive',
                'houseNumber': '123',
                'postalCode': '11101',
                'gpsCoordinates': '-6.8162,39.2803',
                'bankingServices': 'Deposits,Withdrawals,Loans,Foreign Exchange,Safe Deposit',
                'mobileMoneyServices': 'M-Pesa,Airtel Money,Tigo Pesa,Halo Pesa',
                'registrationDate': date(2020, 1, 15),
                'branchStatus': 'Active',
                'closureDate': None,
                'contactPerson': 'John Mwalimu',
                'telephoneNumber': '+255-22-211-8888',
                'altTelephoneNumber': '+255-22-211-8889',
                'branchCategory': 'Head Office'
            },
            {
                'reportingDate': datetime.now(),
                'branchName': 'Kariakoo Branch',
                'taxIdentificationNumber': 'TIN123456789',
                'businessLicense': 'BL000002',
                'branchCode': '002',
                'qrFsrCode': 'QR00000002',
                'region': 'Dar es Salaam',
                'district': 'Ilala',
                'ward': 'Kariakoo',
                'street': 'Msimbazi Street',
                'houseNumber': '456',
                'postalCode': '11102',
                'gpsCoordinates': '-6.8235,39.2695',
                'bankingServices': 'Deposits,Withdrawals,Loans,Foreign Exchange',
                'mobileMoneyServices': 'M-Pesa,Airtel Money,Tigo Pesa',
                'registrationDate': date(2020, 3, 10),
                'branchStatus': 'Active',
                'closureDate': None,
                'contactPerson': 'Mary Kimaro',
                'telephoneNumber': '+255-22-218-5555',
                'altTelephoneNumber': '+255-22-218-5556',
                'branchCategory': 'Grade A Branch'
            },
            {
                'reportingDate': datetime.now(),
                'branchName': 'Mwanza Branch',
                'taxIdentificationNumber': 'TIN123456789',
                'businessLicense': 'BL000003',
                'branchCode': '003',
                'qrFsrCode': 'QR00000003',
                'region': 'Mwanza',
                'district': 'Nyamagana',
                'ward': 'Nyamagana',
                'street': 'Kenyatta Road',
                'houseNumber': '789',
                'postalCode': '33101',
                'gpsCoordinates': '-2.5164,32.9175',
                'bankingServices': 'Deposits,Withdrawals,Loans',
                'mobileMoneyServices': 'M-Pesa,Airtel Money',
                'registrationDate': date(2020, 6, 20),
                'branchStatus': 'Active',
                'closureDate': None,
                'contactPerson': 'Peter Maganga',
                'telephoneNumber': '+255-28-250-3333',
                'altTelephoneNumber': None,
                'branchCategory': 'Grade B Branch'
            }
        ]
        
        # Insert sample data
        insert_query = """
            INSERT INTO "branch" (
                "reportingDate", "branchName", "taxIdentificationNumber", "businessLicense",
                "branchCode", "qrFsrCode", "region", "district", "ward", "street",
                "houseNumber", "postalCode", "gpsCoordinates", "bankingServices",
                "mobileMoneyServices", "registrationDate", "branchStatus", "closureDate",
                "contactPerson", "telephoneNumber", "altTelephoneNumber", "branchCategory"
            ) VALUES (
                %(reportingDate)s, %(branchName)s, %(taxIdentificationNumber)s, %(businessLicense)s,
                %(branchCode)s, %(qrFsrCode)s, %(region)s, %(district)s, %(ward)s, %(street)s,
                %(houseNumber)s, %(postalCode)s, %(gpsCoordinates)s, %(bankingServices)s,
                %(mobileMoneyServices)s, %(registrationDate)s, %(branchStatus)s, %(closureDate)s,
                %(contactPerson)s, %(telephoneNumber)s, %(altTelephoneNumber)s, %(branchCategory)s
            )
        """
        
        print(f"\n📝 Inserting {len(sample_data)} sample branch records...")
        cursor.executemany(insert_query, sample_data)
        conn.commit()
        
        # Verify the data was inserted
        cursor.execute('SELECT COUNT(*) FROM "branch"')
        count = cursor.fetchone()[0]
        print(f"✅ Successfully inserted sample data. Total records: {count}")
        
        # Display the inserted data
        cursor.execute("""
            SELECT "id", "branchCode", "branchName", "region", "district", 
                   "branchStatus", "branchCategory", "contactPerson"
            FROM "branch" 
            ORDER BY "branchCode"
        """)
        
        branches = cursor.fetchall()
        print(f"\n📋 Branch records in database:")
        print("ID | Code | Name | Region | District | Status | Category | Contact")
        print("-" * 80)
        for branch in branches:
            print(f"{branch[0]:2d} | {branch[1]:4s} | {branch[2]:15s} | {branch[3]:10s} | {branch[4]:10s} | {branch[5]:6s} | {branch[6]:12s} | {branch[7]}")
        
    except psycopg2.Error as e:
        print(f"❌ PostgreSQL error: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
    
    return True

if __name__ == "__main__":
    print("🚀 Testing branch table structure and inserting sample data...")
    success = test_branch_table()
    
    if success:
        print("\n✅ Branch table test completed successfully!")
    else:
        print("\n❌ Branch table test failed!")
        exit(1)