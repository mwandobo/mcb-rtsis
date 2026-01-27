#!/usr/bin/env python3
"""
Query personal data records from PostgreSQL
"""

import psycopg2
from config import Config

def query_personal_data():
    """Query and display personal data records"""
    
    config = Config()
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        
        cursor = conn.cursor()
        
        # Get total count
        cursor.execute('SELECT COUNT(*) FROM "personalData"')
        total_count = cursor.fetchone()[0]
        
        print("👤 PERSONAL DATA RECORDS QUERY")
        print("=" * 60)
        print(f"📊 Total records in personalData table: {total_count:,}")
        
        if total_count > 0:
            # Get sample records
            cursor.execute('''
                SELECT "customerIdentificationNumber", "firstName", "middleNames", "otherNames", 
                       "fullNames", "gender", "maritalStatus", "nationality", "profession",
                       "mobileNumber", "emailAddress", "reportingDate"
                FROM "personalData" 
                ORDER BY "customerIdentificationNumber" 
                LIMIT 10
            ''')
            
            records = cursor.fetchall()
            
            print("\n📋 Sample records (first 10):")
            print("-" * 120)
            print(f"{'ID':<8} {'Name':<25} {'Gender':<8} {'Marital':<10} {'Nationality':<15} {'Mobile':<15} {'Email':<25}")
            print("-" * 120)
            
            for record in records:
                cust_id = record[0] or "N/A"
                full_name = record[4] or f"{record[1] or ''} {record[3] or ''}".strip() or "N/A"
                gender = record[5] or "N/A"
                marital = record[6] or "N/A"
                nationality = record[7] or "N/A"
                mobile = record[9] or "N/A"
                email = record[10] or "N/A"
                
                # Truncate long fields
                full_name = full_name[:24] + "..." if len(full_name) > 24 else full_name
                email = email[:24] + "..." if len(email) > 24 else email
                
                print(f"{cust_id:<8} {full_name:<25} {gender:<8} {marital:<10} {nationality:<15} {mobile:<15} {email:<25}")
            
            # Get statistics
            cursor.execute('''
                SELECT 
                    COUNT(CASE WHEN "gender" = 'Male' THEN 1 END) as male_count,
                    COUNT(CASE WHEN "gender" = 'Female' THEN 1 END) as female_count,
                    COUNT(CASE WHEN "maritalStatus" = 'Married' THEN 1 END) as married_count,
                    COUNT(CASE WHEN "maritalStatus" = 'Single' THEN 1 END) as single_count,
                    COUNT(CASE WHEN "nationality" = 'TANZANIA, UNITED REPUBLIC OF' THEN 1 END) as tanzanian_count
                FROM "personalData"
            ''')
            
            stats = cursor.fetchone()
            
            print(f"\n📈 Statistics:")
            print(f"   👨 Male customers: {stats[0]:,}")
            print(f"   👩 Female customers: {stats[1]:,}")
            print(f"   💑 Married customers: {stats[2]:,}")
            print(f"   🙋 Single customers: {stats[3]:,}")
            print(f"   🇹🇿 Tanzanian customers: {stats[4]:,}")
            
            # Get recent records
            cursor.execute('''
                SELECT COUNT(*), DATE("reportingDate") as report_date
                FROM "personalData" 
                GROUP BY DATE("reportingDate")
                ORDER BY report_date DESC
                LIMIT 5
            ''')
            
            recent = cursor.fetchall()
            
            if recent:
                print(f"\n📅 Records by date:")
                for count, date in recent:
                    print(f"   {date}: {count:,} records")
        
        cursor.close()
        conn.close()
        
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Error querying personal data: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    query_personal_data()