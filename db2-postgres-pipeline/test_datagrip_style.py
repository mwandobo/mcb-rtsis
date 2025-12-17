#!/usr/bin/env python3
"""
Test DB2 Connection Using DataGrip-style Parameters
"""

import logging
import pyodbc
from config import Config

def test_datagrip_style_connection():
    """Test DB2 connection using different connection string formats like DataGrip"""
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    config = Config()
    
    logger.info("🔍 Testing DataGrip-style DB2 Connection")
    logger.info("=" * 60)
    logger.info(f"Host: {config.database.db2_host}")
    logger.info(f"Port: {config.database.db2_port}")
    logger.info(f"Database: {config.database.db2_database}")
    logger.info(f"User: {config.database.db2_user}")
    logger.info(f"Schema: {config.database.db2_schema}")
    logger.info("=" * 60)
    
    # Different connection string variations to try
    connection_variations = [
        # Variation 1: Original format
        {
            "name": "Original Format",
            "conn_str": (
                f"DRIVER={{IBM DB2 ODBC DRIVER}};"
                f"DATABASE={config.database.db2_database};"
                f"HOSTNAME={config.database.db2_host};"
                f"PORT={config.database.db2_port};"
                f"PROTOCOL=TCPIP;"
                f"UID={config.database.db2_user};"
                f"PWD={config.database.db2_password};"
                f"CURRENTSCHEMA={config.database.db2_schema};"
            )
        },
        
        # Variation 2: Without CURRENTSCHEMA
        {
            "name": "Without CURRENTSCHEMA",
            "conn_str": (
                f"DRIVER={{IBM DB2 ODBC DRIVER}};"
                f"DATABASE={config.database.db2_database};"
                f"HOSTNAME={config.database.db2_host};"
                f"PORT={config.database.db2_port};"
                f"PROTOCOL=TCPIP;"
                f"UID={config.database.db2_user};"
                f"PWD={config.database.db2_password};"
            )
        },
        
        # Variation 3: Using DB2COPY1 driver
        {
            "name": "DB2COPY1 Driver",
            "conn_str": (
                f"DRIVER={{IBM DB2 ODBC DRIVER - DB2COPY1}};"
                f"DATABASE={config.database.db2_database};"
                f"HOSTNAME={config.database.db2_host};"
                f"PORT={config.database.db2_port};"
                f"PROTOCOL=TCPIP;"
                f"UID={config.database.db2_user};"
                f"PWD={config.database.db2_password};"
            )
        },
        
        # Variation 4: DataGrip-style with SERVER instead of HOSTNAME
        {
            "name": "SERVER instead of HOSTNAME",
            "conn_str": (
                f"DRIVER={{IBM DB2 ODBC DRIVER}};"
                f"DATABASE={config.database.db2_database};"
                f"SERVER={config.database.db2_host};"
                f"PORT={config.database.db2_port};"
                f"PROTOCOL=TCPIP;"
                f"UID={config.database.db2_user};"
                f"PWD={config.database.db2_password};"
            )
        },
        
        # Variation 5: Without PROTOCOL
        {
            "name": "Without PROTOCOL",
            "conn_str": (
                f"DRIVER={{IBM DB2 ODBC DRIVER}};"
                f"DATABASE={config.database.db2_database};"
                f"HOSTNAME={config.database.db2_host};"
                f"PORT={config.database.db2_port};"
                f"UID={config.database.db2_user};"
                f"PWD={config.database.db2_password};"
            )
        },
        
        # Variation 6: Using DSN-style format
        {
            "name": "Minimal Format",
            "conn_str": (
                f"DRIVER={{IBM DB2 ODBC DRIVER}};"
                f"DATABASE={config.database.db2_database};"
                f"HOSTNAME={config.database.db2_host};"
                f"UID={config.database.db2_user};"
                f"PWD={config.database.db2_password};"
            )
        },
        
        # Variation 7: With additional security parameters
        {
            "name": "With Security Parameters",
            "conn_str": (
                f"DRIVER={{IBM DB2 ODBC DRIVER}};"
                f"DATABASE={config.database.db2_database};"
                f"HOSTNAME={config.database.db2_host};"
                f"PORT={config.database.db2_port};"
                f"PROTOCOL=TCPIP;"
                f"UID={config.database.db2_user};"
                f"PWD={config.database.db2_password};"
                f"SECURITY=SSL;"
                f"SSLSERVERCERTIFICATE=;"
            )
        },
    ]
    
    for i, variation in enumerate(connection_variations, 1):
        logger.info(f"\n🔌 Test {i}: {variation['name']}")
        masked_conn_str = variation['conn_str'].replace(config.database.db2_password, '***')
        logger.info(f"   Connection: {masked_conn_str}")
        
        try:
            conn = pyodbc.connect(variation['conn_str'], timeout=15)
            
            # Test with a simple query
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1")
            result = cursor.fetchone()
            
            # Test access to our target table
            try:
                cursor.execute("SELECT COUNT(*) FROM GLI_TRX_EXTRACT FETCH FIRST 1 ROWS ONLY")
                count_result = cursor.fetchone()
                table_access = f"✅ GLI_TRX_EXTRACT accessible (count check passed)"
            except Exception as table_error:
                table_access = f"⚠️ GLI_TRX_EXTRACT error: {str(table_error)[:50]}..."
            
            conn.close()
            
            logger.info(f"✅ SUCCESS! Connection works!")
            logger.info(f"   Server time: {result[0]}")
            logger.info(f"   Table access: {table_access}")
            
            # Update db2_connection.py with working connection string
            update_connection_module(variation['conn_str'])
            return variation['conn_str']
            
        except pyodbc.Error as e:
            error_msg = str(e)
            if "PASSWORD EXPIRED" in error_msg:
                logger.info(f"❌ Password expired")
            elif "INVALID AUTHORIZATION" in error_msg:
                logger.info(f"❌ Invalid authorization")
            elif "timeout" in error_msg.lower():
                logger.info(f"⏱️ Connection timeout")
            elif "SQL30081N" in error_msg:
                logger.info(f"❌ Communication error")
            else:
                logger.info(f"❌ Failed: {error_msg[:60]}...")
        
        except Exception as e:
            logger.info(f"❌ Error: {str(e)[:60]}...")
    
    logger.info("\n" + "=" * 60)
    logger.info("❌ No working connection format found!")
    return None

def update_connection_module(working_conn_str):
    """Update db2_connection.py with the working connection string format"""
    logger = logging.getLogger(__name__)
    
    try:
        # Read the current db2_connection.py
        with open('db2_connection.py', 'r') as f:
            content = f.read()
        
        # Extract the connection string pattern from working format
        # This is a simplified update - in practice you'd want to parse it properly
        logger.info("📝 Working connection string found - manual update needed")
        logger.info(f"   Use this format in db2_connection.py:")
        logger.info(f"   {working_conn_str.replace('{config.database.db2_password}', '{self.config.database.db2_password}')}")
        
    except Exception as e:
        logger.error(f"❌ Failed to update connection module: {e}")

if __name__ == "__main__":
    print("🔍 Testing DataGrip-style DB2 Connection")
    print("=" * 50)
    print("Since your password works in DataGrip, let's find the right connection format...")
    print("=" * 50)
    
    working_format = test_datagrip_style_connection()
    
    if working_format:
        print(f"\n🎉 Found working connection format!")
        print("✅ You can now run the cash pipeline!")
    else:
        print("\n💡 Additional suggestions:")
        print("1. Check DataGrip connection settings (Host, Port, Database)")
        print("2. Look for any special SSL or security settings in DataGrip")
        print("3. Try connecting with a DB2 client tool to verify settings")
        print("4. Check if DataGrip uses a different DB2 driver version")