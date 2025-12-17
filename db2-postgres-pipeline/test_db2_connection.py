#!/usr/bin/env python3
"""
Test DB2 Connection - Diagnose the Real Issue
"""

import logging
import sys
from db2_connection import DB2Connection
from config import Config

def test_db2_connection():
    """Test DB2 connection and diagnose issues"""
    
    # Setup logging
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    logger.info("🔍 Diagnosing DB2 Connection Issues")
    logger.info("=" * 60)
    
    try:
        # Check configuration
        config = Config()
        logger.info("📋 DB2 Configuration:")
        logger.info(f"  Host: {config.database.db2_host}")
        logger.info(f"  Port: {config.database.db2_port}")
        logger.info(f"  Database: {config.database.db2_database}")
        logger.info(f"  User: {config.database.db2_user}")
        logger.info(f"  Password: {'*' * len(config.database.db2_password)}")
        logger.info(f"  Schema: {config.database.db2_schema}")
        
        # Check available drivers
        logger.info("\n🔧 Checking Available ODBC Drivers:")
        try:
            import pyodbc
            drivers = pyodbc.drivers()
            logger.info(f"Available drivers: {drivers}")
            
            # Check for DB2 drivers specifically
            db2_drivers = [d for d in drivers if 'DB2' in d.upper()]
            if db2_drivers:
                logger.info(f"✅ DB2 drivers found: {db2_drivers}")
            else:
                logger.warning("⚠️ No DB2 drivers found!")
                
        except ImportError:
            logger.error("❌ pyodbc not installed!")
            return False
        
        # Test connection
        logger.info("\n🔌 Testing DB2 Connection:")
        db2_conn = DB2Connection()
        
        # Show connection string (masked password)
        conn_str = db2_conn.get_connection_string()
        masked_conn_str = conn_str.replace(config.database.db2_password, '***')
        logger.info(f"Connection string: {masked_conn_str}")
        
        # Attempt connection
        success = db2_conn.test_connection()
        
        if success:
            logger.info("✅ DB2 connection successful!")
            
            # Test a simple query
            logger.info("\n📊 Testing Simple Query:")
            try:
                with db2_conn.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT COUNT(*) FROM GLI_TRX_EXTRACT FETCH FIRST 1 ROWS ONLY")
                    result = cursor.fetchone()
                    logger.info(f"✅ Query test successful: GLI_TRX_EXTRACT accessible")
                    
            except Exception as e:
                logger.error(f"❌ Query test failed: {e}")
                
        else:
            logger.error("❌ DB2 connection failed!")
            return False
            
        return True
        
    except Exception as e:
        logger.error(f"❌ Connection test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_alternative_drivers():
    """Test alternative DB2 connection methods"""
    logger = logging.getLogger(__name__)
    
    logger.info("\n🔄 Testing Alternative Connection Methods:")
    
    # Test ibm_db if available
    try:
        import ibm_db
        logger.info("✅ ibm_db module available")
        
        config = Config()
        conn_str = f"DATABASE={config.database.db2_database};HOSTNAME={config.database.db2_host};PORT={config.database.db2_port};PROTOCOL=TCPIP;UID={config.database.db2_user};PWD={config.database.db2_password};"
        
        logger.info("🔌 Testing ibm_db connection...")
        conn = ibm_db.connect(conn_str, "", "")
        if conn:
            logger.info("✅ ibm_db connection successful!")
            ibm_db.close(conn)
            return True
        else:
            logger.error("❌ ibm_db connection failed!")
            
    except ImportError:
        logger.info("ℹ️ ibm_db module not available")
    except Exception as e:
        logger.error(f"❌ ibm_db connection error: {e}")
    
    return False

if __name__ == "__main__":
    print("🔍 DB2 Connection Diagnostic Tool")
    print("=" * 50)
    
    success = test_db2_connection()
    
    if not success:
        print("\n🔄 Trying alternative methods...")
        test_alternative_drivers()
    
    print("\n" + "=" * 50)
    if success:
        print("✅ DB2 connection is working!")
    else:
        print("❌ DB2 connection issues found - check logs above")