#!/usr/bin/env python3
"""
Verify Channel Record Information Data Quality
Checks for duplicates, data integrity, and provides summary statistics
"""

import sys
import os
import logging
import psycopg2
from collections import defaultdict

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def verify_data_quality():
    """Verify the data quality in the channelRecordInformation table"""
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
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
        
        logger.info("=" * 60)
        logger.info("CHANNEL RECORD INFORMATION DATA QUALITY REPORT")
        logger.info("=" * 60)
        
        # 1. Basic record count
        cursor.execute('SELECT COUNT(*) FROM "channelRecordInformation"')
        total_records = cursor.fetchone()[0]
        logger.info(f"Total records in table: {total_records:,}")
        
        if total_records == 0:
            logger.warning("No records found in table. Pipeline may not have run yet.")
            return
        
        # 2. Check for duplicates by primary identifier (adjust field name as needed)
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'channelRecordInformation' 
            AND column_name IN ('channelId', 'recordId', 'id', 'channelCode')
            ORDER BY ordinal_position
            LIMIT 1
        """)
        primary_field = cursor.fetchone()
        
        if primary_field:
            field_name = primary_field[0]
            cursor.execute(f"""
                SELECT "{field_name}", COUNT(*) as count
                FROM "channelRecordInformation"
                GROUP BY "{field_name}"
                HAVING COUNT(*) > 1
                ORDER BY count DESC
                LIMIT 10
            """)
            duplicates = cursor.fetchall()
            
            if duplicates:
                logger.warning(f"Found {len(duplicates)} duplicate {field_name} values:")
                for identifier, count in duplicates:
                    logger.warning(f"  {field_name} {identifier}: {count} records")
            else:
                logger.info(f"✓ No duplicate {field_name} values found")
        
        # 3. Get table structure for analysis
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns 
            WHERE table_name = 'channelRecordInformation'
            ORDER BY ordinal_position
        """)
        columns = cursor.fetchall()
        
        logger.info(f"\nTable structure analysis:")
        logger.info(f"Total columns: {len(columns)}")
        
        # 4. Data completeness check for key fields
        logger.info(f"\nData completeness check:")
        
        for column_name, data_type in columns:
            if column_name in ['id', 'created_at', 'updated_at']:
                continue  # Skip system fields
                
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM "channelRecordInformation" 
                WHERE "{column_name}" IS NULL OR "{column_name}" = ''
            """)
            null_count = cursor.fetchone()[0]
            completeness = ((total_records - null_count) / total_records) * 100
            status = "✓" if completeness >= 95 else "⚠" if completeness >= 90 else "✗"
            logger.info(f"  {column_name:<30}: {completeness:.1f}% complete {status}")
        
        # 5. Look for common channel-related fields and analyze them
        common_fields = ['channelType', 'channelCategory', 'channelStatus', 'channelName']
        
        for field in common_fields:
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'channelRecordInformation' 
                AND LOWER(column_name) LIKE %s
            """, (f'%{field.lower()}%',))
            
            matching_columns = cursor.fetchall()
            
            for column_tuple in matching_columns:
                column_name = column_tuple[0]
                cursor.execute(f"""
                    SELECT "{column_name}", COUNT(*) as count
                    FROM "channelRecordInformation"
                    WHERE "{column_name}" IS NOT NULL AND "{column_name}" != ''
                    GROUP BY "{column_name}"
                    ORDER BY count DESC
                    LIMIT 10
                """)
                values = cursor.fetchall()
                
                if values:
                    logger.info(f"\n{column_name} distribution:")
                    for value, count in values:
                        percentage = (count / total_records) * 100
                        logger.info(f"  {value}: {count:,} records ({percentage:.1f}%)")
        
        # 6. Date field analysis
        date_fields = []
        for column_name, data_type in columns:
            if 'date' in column_name.lower() or 'time' in column_name.lower():
                date_fields.append(column_name)
        
        if date_fields:
            logger.info(f"\nDate field analysis:")
            for field in date_fields:
                cursor.execute(f"""
                    SELECT 
                        MIN("{field}") as earliest,
                        MAX("{field}") as latest,
                        COUNT(DISTINCT "{field}") as unique_dates
                    FROM "channelRecordInformation"
                    WHERE "{field}" IS NOT NULL AND "{field}" != ''
                """)
                date_info = cursor.fetchone()
                if date_info and date_info[0]:
                    earliest, latest, unique_dates = date_info
                    logger.info(f"  {field}:")
                    logger.info(f"    Earliest: {earliest}")
                    logger.info(f"    Latest: {latest}")
                    logger.info(f"    Unique dates: {unique_dates:,}")
        
        # 7. Numeric field analysis
        numeric_fields = []
        for column_name, data_type in columns:
            if data_type in ['integer', 'bigint', 'numeric', 'decimal', 'real', 'double precision']:
                numeric_fields.append(column_name)
        
        if numeric_fields:
            logger.info(f"\nNumeric field analysis:")
            for field in numeric_fields:
                if field == 'id':
                    continue  # Skip ID field
                    
                cursor.execute(f"""
                    SELECT 
                        COUNT(*) as total_values,
                        AVG(CAST("{field}" AS DECIMAL)) as avg_value,
                        MIN(CAST("{field}" AS DECIMAL)) as min_value,
                        MAX(CAST("{field}" AS DECIMAL)) as max_value
                    FROM "channelRecordInformation"
                    WHERE "{field}" IS NOT NULL 
                    AND "{field}" != ''
                    AND "{field}" ~ '^[0-9]+\\.?[0-9]*$'
                """)
                numeric_info = cursor.fetchone()
                if numeric_info and numeric_info[0] > 0:
                    total_values, avg_value, min_value, max_value = numeric_info
                    logger.info(f"  {field}:")
                    logger.info(f"    Records with values: {total_values:,}")
                    logger.info(f"    Average: {avg_value:.2f}")
                    logger.info(f"    Range: {min_value:.2f} - {max_value:.2f}")
        
        # 8. Recent data check
        cursor.execute("""
            SELECT 
                MIN(created_at) as earliest_created,
                MAX(created_at) as latest_created,
                COUNT(*) as total_records
            FROM "channelRecordInformation"
        """)
        creation_info = cursor.fetchone()
        if creation_info:
            earliest_created, latest_created, total = creation_info
            logger.info(f"\nRecord creation timeline:")
            logger.info(f"  Earliest record: {earliest_created}")
            logger.info(f"  Latest record: {latest_created}")
            logger.info(f"  Total records: {total:,}")
        
        cursor.close()
        conn.close()
        
        logger.info("\n" + "=" * 60)
        logger.info("DATA QUALITY VERIFICATION COMPLETED")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Error during data quality verification: {e}")
        raise

if __name__ == "__main__":
    verify_data_quality()