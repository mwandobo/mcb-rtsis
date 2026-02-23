#!/usr/bin/env python3
"""
Test script to count fields in personal_data_information-v3 query
"""

from db2_connection import DB2Connection
from personal_data_streaming_pipeline import PersonalDataStreamingPipeline

def test_field_count():
    """Test the actual field count returned by the query"""
    
    pipeline = PersonalDataStreamingPipeline(batch_size=1)
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get the query for first batch
            query = pipeline.get_personal_data_query(last_cust_id=None)
            
            print("Executing query to fetch 1 record...")
            cursor.execute(query)
            
            # Fetch one row
            row = cursor.fetchone()
            
            if row:
                print(f"\n✅ Query executed successfully!")
                print(f"📊 Total fields returned: {len(row)}")
                print(f"\nField values (first 10):")
                for i, value in enumerate(row[:10]):
                    print(f"  [{i}] = {value}")
                
                print(f"\n... (showing first 10 of {len(row)} fields)")
                
                print(f"\nLast 5 fields:")
                for i, value in enumerate(row[-5:], start=len(row)-5):
                    print(f"  [{i}] = {value}")
                
                # Check specific fields we're interested in
                print(f"\n🔍 Checking key field positions:")
                print(f"  Field [67] (postalCode): {row[67] if len(row) > 67 else 'OUT OF RANGE'}")
                print(f"  Field [68] (region): {row[68] if len(row) > 68 else 'OUT OF RANGE'}")
                print(f"  Field [69] (district): {row[69] if len(row) > 69 else 'OUT OF RANGE'}")
                print(f"  Field [70] (ward): {row[70] if len(row) > 70 else 'OUT OF RANGE'}")
                print(f"  Field [-1] (cursor_cust_id): {row[-1]}")
                
            else:
                print("❌ No records returned")
                
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_field_count()
