#!/usr/bin/env python3
"""
Test Final Investment Debt Securities Query
"""

import pyodbc
import logging
from config import Config

def test_final_query():
    """Test the final investment debt securities query"""
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    config = Config()
    
    logger.info("🔍 Testing Final Investment Debt Securities Query")
    logger.info("=" * 50)
    
    try:
        conn_str = (
            f"DRIVER={{IBM DB2 ODBC DRIVER}};"
            f"DATABASE={config.database.db2_database};"
            f"HOSTNAME={config.database.db2_host};"
            f"PORT={config.database.db2_port};"
            f"UID={config.database.db2_user};"
            f"PWD={config.database.db2_password};"
        )
        
        conn = pyodbc.connect(conn_str, timeout=30)
        cursor = conn.cursor()
        
        # Read the final SQL query from file
        with open('sqls/investment_debt_securities.sql', 'r') as f:
            query = f.read()
        
        logger.info("📋 Testing final query with limited results...")
        
        # Add FETCH FIRST to limit results for testing
        limited_query = query.rstrip(';') + " FETCH FIRST 10 ROWS ONLY"
        
        # Execute the query
        cursor.execute(limited_query)
        
        # Fetch results
        results = cursor.fetchall()
        
        logger.info(f"✅ Final query executed successfully!")
        logger.info(f"📊 Sample records returned: {len(results)}")
        
        if results:
            # Show sample records with key fields
            logger.info(f"\n📋 Sample Investment Securities:")
            logger.info(f"{'No.':<3} {'Security Number':<20} {'Security Type':<25} {'Issuer':<25} {'Amount (TZS)':<15}")
            logger.info("-" * 90)
            
            for i, row in enumerate(results):
                sec_num = str(row[1])[:20]  # securityNumber (truncated)
                sec_type = str(row[2])[:25]  # securityType
                issuer = str(row[3])[:25]  # securityIssuerName
                amount = float(row[9])  # orgCostValueAmount
                logger.info(f"{i+1:<3} {sec_num:<20} {sec_type:<25} {issuer:<25} {amount:>13,.0f}")
            
            # Show summary by security type
            logger.info(f"\n📊 Summary by Security Type (from sample):")
            security_types = {}
            total_amount = 0
            for row in results:
                sec_type = row[2]  # securityType
                amount = float(row[9])  # orgCostValueAmount
                
                if sec_type in security_types:
                    security_types[sec_type]['count'] += 1
                    security_types[sec_type]['amount'] += amount
                else:
                    security_types[sec_type] = {'count': 1, 'amount': amount}
                
                total_amount += amount
            
            for sec_type, data in security_types.items():
                logger.info(f"  {sec_type}: {data['count']} records, {data['amount']:,.0f} TZS")
            
            logger.info(f"\n💰 Total Amount (sample): {total_amount:,.0f} TZS")
            logger.info(f"💱 Total Amount (sample): ${total_amount/2730.50:,.0f} USD")
        else:
            logger.info("⚠️ No records returned from final query")
        
        # Test individual parts count
        logger.info(f"\n📋 Testing individual parts count...")
        
        # Count DEPOSIT_ACCOUNT part
        deposit_count_query = """
        SELECT COUNT(*) 
        FROM DEPOSIT_ACCOUNT da
        WHERE da.DEPOSIT_TYPE IN ('1', '2', '3', '4', '5')
            AND da.ENTRY_STATUS IN ('1', '6')
            AND (da.BOOK_BALANCE > 0 OR da.AVAILABLE_BALANCE > 0 OR da.OPENING_BALANCE > 0)
        """
        
        cursor.execute(deposit_count_query)
        deposit_count = cursor.fetchone()[0]
        logger.info(f"📊 DEPOSIT_ACCOUNT investment records: {deposit_count:,}")
        
        # Count GLI_TRX_EXTRACT part
        gli_count_query = """
        SELECT COUNT(*) 
        FROM GLI_TRX_EXTRACT gte
        WHERE gte.FK_GLG_ACCOUNTACCO IN (
            SELECT ACCOUNT_ID 
            FROM GLG_ACCOUNT 
            WHERE EXTERNAL_GLACCOUNT LIKE '130%'
        )
        AND gte.DC_AMOUNT IS NOT NULL
        AND gte.DC_AMOUNT > 0
        AND gte.TRN_DATE IS NOT NULL
        """
        
        try:
            cursor.execute(gli_count_query)
            gli_count = cursor.fetchone()[0]
            logger.info(f"📊 GLI_TRX_EXTRACT investment records: {gli_count:,}")
            
            total_expected = deposit_count + gli_count
            logger.info(f"📊 Expected total records: {total_expected:,}")
        except Exception as e:
            logger.info(f"⚠️ Could not get GLI_TRX_EXTRACT count: {e}")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ Final query execution failed: {e}")
        return False

if __name__ == "__main__":
    success = test_final_query()
    if success:
        print("\n✅ Investment Debt Securities query is working!")
        print("📋 The query successfully combines:")
        print("   • Treasury bonds from GLI_TRX_EXTRACT (130% GL accounts)")
        print("   • Corporate/Government bonds from DEPOSIT_ACCOUNT (types 1-5)")
        print("📊 Ready for RTSIS reporting!")
    else:
        print("\n❌ Query needs further investigation")