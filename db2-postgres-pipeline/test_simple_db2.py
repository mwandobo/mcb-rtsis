#!/usr/bin/env python3
"""
Simple DB2 Connection Test - Minimal Approach
"""

import pyodbc
import logging
from config import Config

def test_minimal_connection():
    """Test the most basic DB2 connection possible"""
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    config = Config()
    
    logger.info("🔍 Minimal DB2 Connection Test")
    logger.info("=" * 50)
    logger.info(f"Host: {config.database.db2_host}")
    logger.info(f"Port: {config.database.db2_port}")
    logger.info(f"Database: {config.database.db2_database}")
    logger.info(f"User: {config.database.db2_user}")
    logger.info("=" * 50)
    
    # Test 1: Most basic connection string
    logger.info("\n🔌 Test 1: Basic connection...")
    try:
        conn_str = (
            f"DRIVER={{IBM DB2 ODBC DRIVER}};"
            f"DATABASE={config.database.db2_database};"
            f"HOSTNAME={config.database.db2_host};"
            f"PORT={config.database.db2_port};"
            f"UID={config.database.db2_user};"
            f"PWD={config.database.db2_password};"
        )
        
        logger.info(f"Connection string: {conn_str.replace(config.database.db2_password, '***')}")
        
        conn = pyodbc.connect(conn_str, timeout=30)
        logger.info("✅ Basic connection successful!")
        
        # Test simple query
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1")
        result = cursor.fetchone()
        logger.info(f"✅ Server timestamp: {result[0]}")
        
        # Check DEPOSIT_TYPE values directly
        logger.info("\n🔍 Checking DEPOSIT_TYPE values:")
        try:
            cursor.execute("SELECT DEPOSIT_TYPE, COUNT(*) FROM DEPOSIT_ACCOUNT WHERE DEPOSIT_TYPE IS NOT NULL GROUP BY DEPOSIT_TYPE ORDER BY DEPOSIT_TYPE FETCH FIRST 20 ROWS ONLY")
            results = cursor.fetchall()
            logger.info("DEPOSIT_ACCOUNT DEPOSIT_TYPE values:")
            for deposit_type, count in results:
                logger.info(f'  Type {deposit_type}: {count:>10,} records')
        except Exception as e:
            logger.info(f'DEPOSIT_ACCOUNT DEPOSIT_TYPE: ERROR - {str(e)}')
        
        try:
            cursor.execute("SELECT DEPOSIT_TYPE, COUNT(*) FROM DEPOSIT WHERE DEPOSIT_TYPE IS NOT NULL GROUP BY DEPOSIT_TYPE ORDER BY DEPOSIT_TYPE")
            results = cursor.fetchall()
            logger.info("DEPOSIT DEPOSIT_TYPE values:")
            for deposit_type, count in results:
                logger.info(f'  Type {deposit_type}: {count:>10,} records')
        except Exception as e:
            logger.info(f'DEPOSIT DEPOSIT_TYPE: ERROR - {str(e)}')
        
        # Check ENTRY_STATUS and COLLATERAL_FLG values
        logger.info("\n🔍 Checking ENTRY_STATUS values:")
        try:
            cursor.execute("SELECT ENTRY_STATUS, COUNT(*) FROM DEPOSIT_ACCOUNT WHERE ENTRY_STATUS IS NOT NULL GROUP BY ENTRY_STATUS ORDER BY ENTRY_STATUS FETCH FIRST 10 ROWS ONLY")
            results = cursor.fetchall()
            for status, count in results:
                logger.info(f'  Status {status}: {count:>10,} records')
        except Exception as e:
            logger.info(f'ENTRY_STATUS check: ERROR - {str(e)}')
        
        logger.info("\n🔍 Checking COLLATERAL_FLG values:")
        try:
            cursor.execute("SELECT COLLATERAL_FLG, COUNT(*) FROM DEPOSIT_ACCOUNT WHERE COLLATERAL_FLG IS NOT NULL GROUP BY COLLATERAL_FLG ORDER BY COLLATERAL_FLG")
            results = cursor.fetchall()
            for flag, count in results:
                logger.info(f'  Flag {flag}: {count:>10,} records')
        except Exception as e:
            logger.info(f'COLLATERAL_FLG check: ERROR - {str(e)}')
        
        # Check for lookup tables
        logger.info("\n🔍 Checking for lookup tables:")
        lookup_queries = [
            ("GENERIC_DETAIL DEPST", "SELECT SERIAL_NUM, DESCRIPTION FROM GENERIC_DETAIL WHERE PARAMETER_TYPE = 'DEPST' ORDER BY SERIAL_NUM FETCH FIRST 10 ROWS ONLY"),
            ("GENERIC_DETAIL DSTAT", "SELECT SERIAL_NUM, DESCRIPTION FROM GENERIC_DETAIL WHERE PARAMETER_TYPE = 'DSTAT' ORDER BY SERIAL_NUM FETCH FIRST 10 ROWS ONLY"),
            ("GENERIC_DETAIL DTYPE", "SELECT SERIAL_NUM, DESCRIPTION FROM GENERIC_DETAIL WHERE PARAMETER_TYPE = 'DTYPE' ORDER BY SERIAL_NUM FETCH FIRST 10 ROWS ONLY"),
        ]
        
        for desc, query in lookup_queries:
            try:
                cursor.execute(query)
                results = cursor.fetchall()
                if results:
                    logger.info(f'{desc}:')
                    for code, description in results:
                        logger.info(f'  {code}: {description}')
                else:
                    logger.info(f'{desc}: No records found')
            except Exception as e:
                logger.info(f'{desc}: ERROR - {str(e)[:50]}...')
        
        # Check all parameter types in GENERIC_DETAIL
        logger.info("\n🔍 Checking all GENERIC_DETAIL parameter types:")
        try:
            cursor.execute("SELECT PARAMETER_TYPE, COUNT(*) FROM GENERIC_DETAIL GROUP BY PARAMETER_TYPE ORDER BY PARAMETER_TYPE FETCH FIRST 50 ROWS ONLY")
            results = cursor.fetchall()
            for param_type, count in results:
                logger.info(f'  {param_type}: {count} records')
        except Exception as e:
            logger.info(f'All parameter types check: ERROR - {str(e)}')
        
        # Check specific lookups that might exist
        logger.info("\n🔍 Checking specific lookup attempts:")
        specific_lookups = [
            ("GENERIC_DETAIL ACCTP", "SELECT SERIAL_NUM, DESCRIPTION FROM GENERIC_DETAIL WHERE PARAMETER_TYPE = 'ACCTP' ORDER BY SERIAL_NUM FETCH FIRST 10 ROWS ONLY"),
            ("GENERIC_DETAIL ACCDP", "SELECT SERIAL_NUM, DESCRIPTION FROM GENERIC_DETAIL WHERE PARAMETER_TYPE = 'ACCDP' ORDER BY SERIAL_NUM FETCH FIRST 10 ROWS ONLY"),
            ("GENERIC_DETAIL DEPAR", "SELECT SERIAL_NUM, DESCRIPTION FROM GENERIC_DETAIL WHERE PARAMETER_TYPE = 'DEPAR' ORDER BY SERIAL_NUM FETCH FIRST 10 ROWS ONLY"),
        ]
        
        for desc, query in specific_lookups:
            try:
                cursor.execute(query)
                results = cursor.fetchall()
                if results:
                    logger.info(f'{desc}:')
                    for code, description in results:
                        logger.info(f'  {code}: {description}')
                else:
                    logger.info(f'{desc}: No records found')
            except Exception as e:
                logger.info(f'{desc}: ERROR - {str(e)[:50]}...')
        
        for desc, query in securities_queries:
            try:
                cursor.execute(query)
                count = cursor.fetchone()[0]
                logger.info(f'{desc:<30}: {count:>15,} records')
            except Exception as e:
                logger.info(f'{desc:<30}: ERROR - {str(e)[:50]}...')
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ Basic connection failed: {e}")
    
    # Test 2: Try with longer timeout
    logger.info("\n🔌 Test 2: Extended timeout...")
    try:
        conn_str = (
            f"DRIVER={{IBM DB2 ODBC DRIVER}};"
            f"DATABASE={config.database.db2_database};"
            f"HOSTNAME={config.database.db2_host};"
            f"PORT={config.database.db2_port};"
            f"UID={config.database.db2_user};"
            f"PWD={config.database.db2_password};"
            f"CONNECTTIMEOUT=60;"
            f"LOGINTIMEOUT=60;"
        )
        
        conn = pyodbc.connect(conn_str, timeout=60)
        logger.info("✅ Extended timeout connection successful!")
        
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1")
        result = cursor.fetchone()
        logger.info(f"✅ Server timestamp: {result[0]}")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ Extended timeout failed: {e}")
    
    # Test 3: Check if it's a recent password expiration
    logger.info("\n🔌 Test 3: Check password status...")
    try:
        # Sometimes the error message gives more details
        conn_str = (
            f"DRIVER={{IBM DB2 ODBC DRIVER}};"
            f"DATABASE={config.database.db2_database};"
            f"HOSTNAME={config.database.db2_host};"
            f"PORT={config.database.db2_port};"
            f"UID={config.database.db2_user};"
            f"PWD={config.database.db2_password};"
        )
        
        conn = pyodbc.connect(conn_str)
        
    except pyodbc.Error as e:
        error_code = e.args[0] if e.args else "Unknown"
        error_msg = e.args[1] if len(e.args) > 1 else str(e)
        
        logger.info(f"Error Code: {error_code}")
        logger.info(f"Error Message: {error_msg}")
        
        if "PASSWORD EXPIRED" in error_msg:
            logger.info("💡 The password has expired recently")
            logger.info("💡 This might have happened between your last successful run and now")
        elif "INVALID AUTHORIZATION" in error_msg:
            logger.info("💡 Authorization failed - password might be wrong")
        elif "SQL30082N" in error_msg:
            logger.info("💡 Security processing failed")
        
        return False
    
    return False

def check_system_changes():
    """Check for recent system changes that might affect DB2 connection"""
    logger = logging.getLogger(__name__)
    
    logger.info("\n🔍 Checking for system changes...")
    
    try:
        # Check pyodbc version
        logger.info(f"pyodbc version: {pyodbc.version}")
        
        # Check available drivers
        drivers = pyodbc.drivers()
        db2_drivers = [d for d in drivers if 'DB2' in d.upper()]
        logger.info(f"DB2 drivers: {db2_drivers}")
        
        # Check if there are multiple DB2 driver versions
        if len(db2_drivers) > 1:
            logger.info("💡 Multiple DB2 drivers found - this might cause conflicts")
        
    except Exception as e:
        logger.error(f"System check error: {e}")

if __name__ == "__main__":
    print("🔍 Simple DB2 Connection Test")
    print("=" * 40)
    print("Testing if the connection worked before but stopped recently...")
    print("=" * 40)
    
    success = test_minimal_connection()
    check_system_changes()
    
    if not success:
        print("\n💡 Possible reasons why it stopped working:")
        print("1. Password expired recently (most likely)")
        print("2. DB2 server configuration changed")
        print("3. Network/firewall changes")
        print("4. DB2 client driver updated")
        print("5. Windows updates affected ODBC drivers")
        print("\n💼 Next steps:")
        print("1. Check with DB2 admin if password expired today/recently")
        print("2. Try connecting with DataGrip right now to confirm it still works")
        print("3. If DataGrip works, we'll investigate driver differences")
    else:
        print("\n✅ Connection is working! The issue might be elsewhere.")