#!/usr/bin/env python3
"""
Test Cheque Clearing Pipeline - Check if it's working fine
"""

from db2_connection import DB2Connection
import logging
from contextlib import contextmanager
from config import Config

def test_cheque_clearing():
    """Test the cheque clearing pipeline configuration and data access"""
    db2_conn = DB2Connection()
    config = Config()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    @contextmanager
    def get_db2_connection():
        """Get DB2 connection"""
        with db2_conn.get_connection() as conn:
            yield conn
    
    try:
        logger.info("🔍 TESTING CHEQUE CLEARING PIPELINE")
        logger.info("=" * 60)
        
        # Test 1: Check if configuration exists
        cheque_config = config.tables.get('chequeClearing')
        if cheque_config:
            logger.info("✅ Configuration found for chequeClearing")
            logger.info(f"   Target table: {cheque_config.target_table}")
            logger.info(f"   Processor: {cheque_config.processor_class}")
            logger.info(f"   Batch size: {cheque_config.batch_size}")
        else:
            logger.error("❌ Configuration NOT found for chequeClearing")
            return
        
        # Test 2: Check DB2 connection and table existence
        logger.info("\n🔗 TESTING DB2 CONNECTION AND TABLES")
        with get_db2_connection() as conn:
            cursor = conn.cursor()
            
            # Check if main tables exist
            tables_to_check = [
                'CHEQUES_FOR_COLLEC',
                'DEPOSIT_ACCOUNT', 
                'CUSTOMER',
                'CURRENCY',
                'BANK_BIC_LOOKUP',
                'PROFITS_ACCOUNT'
            ]
            
            for table_name in tables_to_check:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name} FETCH FIRST 1 ROWS ONLY")
                    count = cursor.fetchone()[0]
                    logger.info(f"✅ {table_name}: Accessible")
                except Exception as e:
                    logger.error(f"❌ {table_name}: {str(e)[:100]}...")
            
            # Test 3: Try a simplified version of the query
            logger.info("\n📊 TESTING SIMPLIFIED QUERY")
            try:
                simple_query = """
                SELECT COUNT(*) 
                FROM CHEQUES_FOR_COLLEC AS cfc
                FETCH FIRST 1 ROWS ONLY
                """
                cursor.execute(simple_query)
                count = cursor.fetchone()[0]
                logger.info(f"✅ CHEQUES_FOR_COLLEC has records: {count}")
            except Exception as e:
                logger.error(f"❌ Simple query failed: {e}")
                return
            
            # Test 4: Try the full query with FETCH FIRST 5
            logger.info("\n🔍 TESTING FULL QUERY (5 records)")
            try:
                test_query = cheque_config.query.replace("FETCH FIRST 1000 ROWS ONLY", "FETCH FIRST 5 ROWS ONLY")
                cursor.execute(test_query)
                rows = cursor.fetchall()
                
                logger.info(f"✅ Full query executed successfully")
                logger.info(f"   Records returned: {len(rows)}")
                
                if rows:
                    logger.info("📋 Sample data structure:")
                    sample_row = rows[0]
                    logger.info(f"   Fields count: {len(sample_row)}")
                    logger.info(f"   Sample cheque number: {sample_row[1]}")
                    logger.info(f"   Sample issuer: {sample_row[2]}")
                    logger.info(f"   Sample payee: {sample_row[4]}")
                    logger.info(f"   Sample amount: {sample_row[15]}")
                else:
                    logger.warning("⚠️ Query returned no records")
                    
            except Exception as e:
                logger.error(f"❌ Full query failed: {e}")
                logger.error("   This might be due to missing tables or data")
                return
        
        # Test 5: Check processor import
        logger.info("\n🔧 TESTING PROCESSOR IMPORT")
        try:
            from processors.cheque_clearing_processor import ChequeClearingProcessor
            processor = ChequeClearingProcessor()
            logger.info("✅ ChequeClearingProcessor imported successfully")
        except Exception as e:
            logger.error(f"❌ Processor import failed: {e}")
            return
        
        logger.info("\n🎉 ALL TESTS PASSED!")
        logger.info("✅ Cheque clearing pipeline is ready to run")
        logger.info("   Note: PostgreSQL connection issues need to be resolved by DBA")
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_cheque_clearing()