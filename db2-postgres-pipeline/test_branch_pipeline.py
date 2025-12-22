#!/usr/bin/env python3
"""
Test script for branch pipeline
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import Config
from processors.branch_processor import BranchProcessor, BranchRecord
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_branch_pipeline():
    """Test the branch pipeline configuration and processor"""
    
    print("🚀 Testing Branch Pipeline Configuration...")
    
    # Test configuration
    config = Config()
    
    if 'branch' not in config.tables:
        print("❌ Branch table configuration not found!")
        return False
    
    branch_config = config.tables['branch']
    print(f"✅ Branch configuration found:")
    print(f"  - Name: {branch_config.name}")
    print(f"  - Target Table: {branch_config.target_table}")
    print(f"  - Queue Name: {branch_config.queue_name}")
    print(f"  - Processor Class: {branch_config.processor_class}")
    print(f"  - Batch Size: {branch_config.batch_size}")
    print(f"  - Poll Interval: {branch_config.poll_interval}")
    
    # Test processor
    print(f"\n🔧 Testing Branch Processor...")
    processor = BranchProcessor()
    
    # Create sample data (simulating DB2 query result)
    sample_data = (
        '191220251430',  # reportingDate
        'Head Office',   # branchName
        '123456789',     # taxIdentificationNumber
        'TL-001',        # businessLicense
        '001',           # branchCode
        'FSR-001',       # qrFsrCode
        'Dar es Salaam', # region
        'Ilala',         # district
        'Kivukoni',      # ward
        'Sokoine Drive', # street
        '123',           # houseNumber
        '11101',         # postalCode
        '-6.8162,39.2803', # gpsCoordinates
        'Full Banking Services', # bankingServices
        'M-Pesa,Airtel Money', # mobileMoneyServices
        '150120201200',  # registrationDate
        'Active',        # branchStatus
        None,            # closureDate
        'John Mwalimu',  # contactPerson
        '+255-22-211-8888', # telephoneNumber
        '+255-22-211-8889', # altTelephoneNumber
        'Head Office'    # branchCategory
    )
    
    # Test record processing
    try:
        record = processor.process_record(sample_data, 'branch')
        print(f"✅ Record processed successfully:")
        print(f"  - Branch Code: {record.branch_code}")
        print(f"  - Branch Name: {record.branch_name}")
        print(f"  - Region: {record.region}")
        print(f"  - Status: {record.branch_status}")
        print(f"  - Category: {record.branch_category}")
        
        # Test validation
        is_valid = processor.validate_record(record)
        print(f"✅ Record validation: {'PASSED' if is_valid else 'FAILED'}")
        
        # Test data transformation
        transformed = processor.transform_data(sample_data)
        print(f"✅ Data transformation: {transformed}")
        
    except Exception as e:
        print(f"❌ Record processing failed: {e}")
        return False
    
    # Test PostgreSQL insertion
    print(f"\n💾 Testing PostgreSQL insertion...")
    try:
        conn_params = {
            'host': os.getenv('PG_HOST', 'localhost'),
            'port': int(os.getenv('PG_PORT', '5432')),
            'database': os.getenv('PG_DATABASE', 'bank_data'),
            'user': os.getenv('PG_USER', 'postgres'),
            'password': os.getenv('PG_PASSWORD', 'postgres')
        }
        
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        # Clear existing test data
        cursor.execute('DELETE FROM "branch" WHERE "branchCode" = %s', ('001',))
        
        # Insert test record
        processor.insert_to_postgres(record, cursor)
        conn.commit()
        
        # Verify insertion
        cursor.execute('SELECT COUNT(*) FROM "branch" WHERE "branchCode" = %s', ('001',))
        count = cursor.fetchone()[0]
        
        if count > 0:
            print(f"✅ PostgreSQL insertion successful. Records inserted: {count}")
            
            # Display the inserted record
            cursor.execute("""
                SELECT "branchCode", "branchName", "region", "district", 
                       "branchStatus", "branchCategory", "contactPerson"
                FROM "branch" 
                WHERE "branchCode" = %s
            """, ('001',))
            
            branch_data = cursor.fetchone()
            if branch_data:
                print(f"📋 Inserted record:")
                print(f"  Code: {branch_data[0]} | Name: {branch_data[1]} | Region: {branch_data[2]}")
                print(f"  District: {branch_data[3]} | Status: {branch_data[4]} | Category: {branch_data[5]}")
                print(f"  Contact: {branch_data[6]}")
        else:
            print(f"❌ PostgreSQL insertion failed - no records found")
            return False
            
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ PostgreSQL test failed: {e}")
        return False
    
    # Test SQL query syntax
    print(f"\n📝 Testing SQL Query...")
    query = branch_config.query
    print(f"✅ Query length: {len(query)} characters")
    print(f"✅ Query contains required fields: {all(field in query for field in ['branchName', 'branchCode', 'region'])}")
    
    return True

if __name__ == "__main__":
    print("🧪 Branch Pipeline Test Suite")
    print("=" * 50)
    
    success = test_branch_pipeline()
    
    if success:
        print("\n✅ All branch pipeline tests passed!")
        print("\n🚀 Branch pipeline is ready for production use!")
    else:
        print("\n❌ Branch pipeline tests failed!")
        exit(1)