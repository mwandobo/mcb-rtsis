#!/usr/bin/env python3
"""
Test upsert functionality for card transactions
"""

import psycopg2
from config import Config
from processors.card_transaction_processor import CardTransactionProcessor, CardTransactionRecord
from datetime import datetime

def test_upsert():
    """Test the upsert functionality"""
    config = Config()
    processor = CardTransactionProcessor()
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=config.database.pg_host,
        port=config.database.pg_port,
        database=config.database.pg_database,
        user=config.database.pg_user,
        password=config.database.pg_password
    )
    
    cursor = conn.cursor()
    
    # Create a test record
    test_record = CardTransactionRecord(
        source_table="test",
        timestamp_column_value="2024-01-01",
        reporting_date="2024-01-01",
        card_number="1234567890123456",
        bin_number="123456",
        transacting_bank_name="Test Bank",
        transaction_id="TEST123456",
        transaction_date="2024-01-01",
        transaction_nature="Test Transaction",
        atm_code=None,
        pos_number=None,
        transaction_description="Test Description",
        beneficiary_name="Test Beneficiary",
        beneficiary_trade_name=None,
        beneficiary_country="Tanzania",
        transaction_place="Test Place",
        qty_items_purchased=None,
        unit_price=None,
        org_facilitator_commission_amount=None,
        usd_facilitator_commission_amount=None,
        tzs_facilitator_commission_amount=None,
        currency="TZS",
        org_transaction_amount="1000.00",
        usd_transaction_amount="0.40",
        tzs_transaction_amount="1000.00",
        original_timestamp=datetime.now().isoformat()
    )
    
    print("🧪 Testing upsert functionality...")
    
    # First insert
    print("1️⃣ First insert...")
    try:
        processor.insert_to_postgres(test_record, cursor)
        conn.commit()
        print("✅ First insert successful")
    except Exception as e:
        print(f"❌ First insert failed: {e}")
        conn.rollback()
        return
    
    # Second insert (should update, not fail)
    print("2️⃣ Second insert (should update)...")
    test_record.beneficiary_name = "Updated Beneficiary"
    try:
        processor.insert_to_postgres(test_record, cursor)
        conn.commit()
        print("✅ Second insert/update successful")
    except Exception as e:
        print(f"❌ Second insert failed: {e}")
        conn.rollback()
        return
    
    # Verify the record
    print("3️⃣ Verifying record...")
    cursor.execute('SELECT "beneficiaryName" FROM "cardTransaction" WHERE "transactionId" = %s', (test_record.transaction_id,))
    result = cursor.fetchone()
    
    if result and result[0] == "Updated Beneficiary":
        print("✅ Upsert working correctly - record was updated")
    else:
        print(f"❌ Upsert not working - expected 'Updated Beneficiary', got: {result}")
    
    # Clean up
    print("4️⃣ Cleaning up test record...")
    cursor.execute('DELETE FROM "cardTransaction" WHERE "transactionId" = %s', (test_record.transaction_id,))
    conn.commit()
    print("✅ Test record cleaned up")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    test_upsert()