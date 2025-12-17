#!/usr/bin/env python3
"""
Test with exact DataGrip-style parameters
"""

import pyodbc
import logging

def test_datagrip_exact():
    """Test with parameters that might match DataGrip exactly"""
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    logger.info("🔍 Testing DataGrip-exact Connection Parameters")
    logger.info("=" * 60)
    
    # Test different combinations that DataGrip might use
    test_configs = [
        {
            "name": "Standard ODBC",
            "host": "172.10.2.42",
            "port": "50000",
            "database": "mcbecho",
            "user": "profits",
            "password": "prft2016"
        },
        {
            "name": "Alternative case",
            "host": "172.10.2.42", 
            "port": "50000",
            "database": "MCBECHO",  # Uppercase
            "user": "PROFITS",      # Uppercase
            "password": "prft2016"
        },
        {
            "name": "With schema",
            "host": "172.10.2.42",
            "port": "50000", 
            "database": "mcbecho",
            "user": "profits",
            "password": "prft2016",
            "schema": "PROFITS"
        }
    ]
    
    for config in test_configs:
        logger.info(f"\n🔌 Testing: {config['name']}")
        logger.info(f"   Host: {config['host']}")
        logger.info(f"   Database: {config['database']}")
        logger.info(f"   User: {config['user']}")
        
        try:
            # Build connection string
            conn_str = (
                f"DRIVER={{IBM DB2 ODBC DRIVER}};"
                f"DATABASE={config['database']};"
                f"HOSTNAME={config['host']};"
                f"PORT={config['port']};"
                f"UID={config['user']};"
                f"PWD={config['password']};"
            )
            
            if 'schema' in config:
                conn_str += f"CURRENTSCHEMA={config['schema']};"
            
            conn = pyodbc.connect(conn_str, timeout=15)
            
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1")
            result = cursor.fetchone()
            
            logger.info(f"✅ SUCCESS! {config['name']} works!")
            logger.info(f"   Server time: {result[0]}")
            
            conn.close()
            return config
            
        except Exception as e:
            error_msg = str(e)
            if "PASSWORD EXPIRED" in error_msg:
                logger.info(f"❌ Password expired")
            else:
                logger.info(f"❌ Failed: {error_msg[:60]}...")
    
    return None

if __name__ == "__main__":
    print("🔍 Testing DataGrip-exact Parameters")
    print("=" * 50)
    
    working_config = test_datagrip_exact()
    
    if working_config:
        print(f"\n✅ Found working configuration: {working_config['name']}")
    else:
        print("\n❌ No configuration worked")
        print("\n💡 Please check DataGrip connection right now:")
        print("1. Open DataGrip")
        print("2. Try to connect to your DB2 database")
        print("3. Run: SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1")
        print("4. If it works, copy the exact connection parameters")
        print("5. If it fails, the password has expired for everyone")