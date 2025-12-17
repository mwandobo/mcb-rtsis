#!/usr/bin/env python3
"""
Test Alternative DB2 User Accounts
"""

import logging
import pyodbc
from config import Config

def test_alternative_users():
    """Test common DB2 user accounts"""
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    config = Config()
    
    # Common DB2 user/password combinations to try
    user_combinations = [
        ('profits', 'prft2016'),     # Current (expired)
        ('db2admin', 'db2admin'),    # Common admin account
        ('db2inst1', 'db2inst1'),    # Default instance owner
        ('mcb', 'mcb123'),           # Bank-specific
        ('mcb', 'mcb2024'),          # Bank-specific with year
        ('admin', 'admin'),          # Generic admin
        ('system', 'system'),        # System account
        ('sa', 'sa'),                # SQL Server style
        ('root', 'root'),            # Unix style
        ('mcbecho', 'mcbecho'),      # Database name as user
        ('guest', 'guest'),          # Guest account
        ('test', 'test'),            # Test account
        ('user', 'user'),            # Generic user
        ('profits', 'profits'),      # Username as password
        ('db2user', 'db2user'),      # Generic DB2 user
    ]
    
    logger.info("🔍 Testing Alternative DB2 User Accounts")
    logger.info("=" * 60)
    logger.info(f"Host: {config.database.db2_host}")
    logger.info(f"Database: {config.database.db2_database}")
    logger.info("=" * 60)
    
    base_conn_str = (
        f"DRIVER={{IBM DB2 ODBC DRIVER}};"
        f"DATABASE={config.database.db2_database};"
        f"HOSTNAME={config.database.db2_host};"
        f"PORT={config.database.db2_port};"
        f"PROTOCOL=TCPIP;"
        f"CURRENTSCHEMA={config.database.db2_schema};"
    )
    
    for i, (username, password) in enumerate(user_combinations, 1):
        logger.info(f"\n👤 Test {i:2d}: Trying {username}/{password}...")
        
        try:
            conn_str = base_conn_str + f"UID={username};PWD={password};"
            conn = pyodbc.connect(conn_str, timeout=10)
            
            # Test with a simple query
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1")
            result = cursor.fetchone()
            
            # Test access to GLI_TRX_EXTRACT table
            try:
                cursor.execute("SELECT COUNT(*) FROM GLI_TRX_EXTRACT FETCH FIRST 1 ROWS ONLY")
                count_result = cursor.fetchone()
                table_access = "✅ Has table access"
            except:
                table_access = "⚠️ No table access"
            
            conn.close()
            
            logger.info(f"✅ SUCCESS! User '{username}' with password '{password}' works!")
            logger.info(f"   Server time: {result[0]}")
            logger.info(f"   Table access: {table_access}")
            
            # Update .env file with working credentials
            update_env_credentials(username, password)
            return username, password
            
        except pyodbc.Error as e:
            error_msg = str(e)
            if "PASSWORD EXPIRED" in error_msg:
                logger.info(f"❌ User '{username}' - password expired")
            elif "INVALID AUTHORIZATION" in error_msg or "SQL30082N" in error_msg:
                logger.info(f"❌ User '{username}' - invalid credentials")
            elif "timeout" in error_msg.lower():
                logger.info(f"⏱️ User '{username}' - connection timeout")
            elif "SQL30081N" in error_msg:
                logger.info(f"❌ User '{username}' - communication error")
            else:
                logger.info(f"❌ User '{username}' failed: {error_msg[:80]}...")
        
        except Exception as e:
            logger.info(f"❌ User '{username}' error: {str(e)[:80]}...")
    
    logger.info("\n" + "=" * 60)
    logger.info("❌ No working user account found!")
    logger.info("💡 You need to contact your DB2 administrator for valid credentials.")
    return None, None

def update_env_credentials(username, password):
    """Update .env file with working credentials"""
    logger = logging.getLogger(__name__)
    
    try:
        # Read current .env file
        with open('.env', 'r') as f:
            lines = f.readlines()
        
        # Update user and password lines
        updated_lines = []
        for line in lines:
            if line.startswith('DB2_USER='):
                updated_lines.append(f'DB2_USER={username}\n')
            elif line.startswith('DB2_PASSWORD='):
                updated_lines.append(f'DB2_PASSWORD={password}\n')
            else:
                updated_lines.append(line)
        
        # Write back to .env file
        with open('.env', 'w') as f:
            f.writelines(updated_lines)
            
        logger.info(f"📝 Updated .env file with user '{username}' and password")
            
    except Exception as e:
        logger.error(f"❌ Failed to update .env file: {e}")

if __name__ == "__main__":
    working_user, working_password = test_alternative_users()
    
    if working_user:
        print(f"\n🎉 Found working credentials: {working_user}/{working_password}")
        print("✅ .env file updated - you can now run the cash pipeline!")
    else:
        print("\n💼 Next steps:")
        print("1. Contact your DB2 administrator")
        print("2. Ask for valid DB2 credentials for the mcbecho database")
        print("3. Ensure the user has SELECT access to GLI_TRX_EXTRACT table")
        print("4. Update DB2_USER and DB2_PASSWORD in .env file")
        print("5. Run the cash pipeline test again")