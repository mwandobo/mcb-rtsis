#!/usr/bin/env python3
"""
Query Personal Data Corporate table
Simple script to check the personal data corporate records in PostgreSQL
"""

import os
import sys
import psycopg2
from datetime import datetime

# Add the current directory to Python path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import Config

def query_personal_data_corporate():
    """Query and display personal data corporate information"""
    
    try:
        # Connect to PostgreSQL
        config = Config()
        pg_conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        pg_cursor = pg_conn.cursor()
        
        print("🏢 Personal Data Corporate Query Results")
        print("=" * 60)
        
        # Get total count
        pg_cursor.execute('SELECT COUNT(*) FROM "personalDataCorporate"')
        total_count = pg_cursor.fetchone()[0]
        print(f"📊 Total corporate records: {total_count}")
        
        if total_count > 0:
            # Get sample records
            pg_cursor.execute('''
                SELECT 
                    "customerIdentificationNumber",
                    "companyName",
                    "tradeName",
                    "legalForm",
                    "numberOfEmployees",
                    "registrationCountry",
                    "taxIdentificationNumber",
                    "fullName",
                    "mobileNumber",
                    "entityType"
                FROM "personalDataCorporate" 
                ORDER BY "customerIdentificationNumber"
                LIMIT 10
            ''')
            
            records = pg_cursor.fetchall()
            
            print(f"\n📋 Sample Records (showing {len(records)} of {total_count}):")
            print("-" * 120)
            print(f"{'Customer ID':<15} {'Company Name':<25} {'Trade Name':<20} {'Legal Form':<15} {'Employees':<10} {'Country':<15}")
            print("-" * 120)
            
            for record in records:
                cust_id = record[0] or 'N/A'
                company_name = (record[1] or 'N/A')[:24]
                trade_name = (record[2] or 'N/A')[:19]
                legal_form = (record[3] or 'N/A')[:14]
                employees = str(record[4]) if record[4] is not None else 'N/A'
                country = (record[5] or 'N/A')[:14]
                
                print(f"{cust_id:<15} {company_name:<25} {trade_name:<20} {legal_form:<15} {employees:<10} {country:<15}")
            
            # Get statistics by legal form
            print(f"\n📈 Statistics by Legal Form:")
            print("-" * 40)
            pg_cursor.execute('''
                SELECT 
                    "legalForm",
                    COUNT(*) as count,
                    AVG("numberOfEmployees") as avg_employees
                FROM "personalDataCorporate" 
                WHERE "legalForm" IS NOT NULL
                GROUP BY "legalForm"
                ORDER BY count DESC
            ''')
            
            stats = pg_cursor.fetchall()
            for legal_form, count, avg_emp in stats:
                avg_emp_str = f"{avg_emp:.1f}" if avg_emp else "N/A"
                print(f"{legal_form:<30} {count:>5} records (Avg employees: {avg_emp_str})")
            
            # Get statistics by country
            print(f"\n🌍 Statistics by Registration Country:")
            print("-" * 50)
            pg_cursor.execute('''
                SELECT 
                    "registrationCountry",
                    COUNT(*) as count
                FROM "personalDataCorporate" 
                WHERE "registrationCountry" IS NOT NULL
                GROUP BY "registrationCountry"
                ORDER BY count DESC
            ''')
            
            country_stats = pg_cursor.fetchall()
            for country, count in country_stats:
                print(f"{country:<35} {count:>5} records")
            
            # Get recent records
            print(f"\n🕒 Most Recent Records:")
            print("-" * 80)
            pg_cursor.execute('''
                SELECT 
                    "customerIdentificationNumber",
                    "companyName",
                    "establishmentDate",
                    created_at
                FROM "personalDataCorporate" 
                ORDER BY created_at DESC
                LIMIT 5
            ''')
            
            recent_records = pg_cursor.fetchall()
            print(f"{'Customer ID':<15} {'Company Name':<30} {'Established':<12} {'Created At':<20}")
            print("-" * 80)
            
            for record in recent_records:
                cust_id = record[0] or 'N/A'
                company_name = (record[1] or 'N/A')[:29]
                est_date = str(record[2]) if record[2] else 'N/A'
                created_at = record[3].strftime('%Y-%m-%d %H:%M') if record[3] else 'N/A'
                
                print(f"{cust_id:<15} {company_name:<30} {est_date:<12} {created_at:<20}")
        
        else:
            print("📭 No corporate records found in the database")
            print("\n💡 To populate data, run:")
            print("   python run_personal_data_corporate_pipeline.py")
        
        print(f"\n✅ Query completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"❌ Error querying database: {e}")
        return False
    finally:
        if 'pg_cursor' in locals():
            pg_cursor.close()
        if 'pg_conn' in locals():
            pg_conn.close()
    
    return True

if __name__ == "__main__":
    query_personal_data_corporate()