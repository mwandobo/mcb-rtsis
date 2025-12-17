#!/usr/bin/env python3
"""
Test Different Password Variations for DB2
"""

import logging
import pyodbc
from config import Config

def test_password_variations():
    """Test common password variations"""
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    config = Config()
    
    # Common password variations to try
    password_variations = [
        'prft2016',      # Current (expired)
        'prft2017',      # Next year
        'prft2024',      # Current year
        'prft2025',      # Next year
        'profits2024',   # Full word + year
        'profits2025',   # Full word + next year
        'Prft2024!',     # Capitalized with special char
        'Profits123!',   # Common pattern
        'prft123',       # Simple number
        'profits',       # Just the username
        'password',      # Default
        'admin',         # Common admin password
    ]
    
    logger.info("🔍 Testing Password Variations for DB2")
    logger.info("=" * 50)
    logger.info(f"Host: {config.database.db2_host}")
    logger.info(f"User: {config.database.db2_user}")
    logger.info("=" * 50)
    
    base_conn_str = (
        f"DRIVER={{IBM DB2 ODBC DRIVER}};"
        f"DATABASE={config.database.db2_database};"
        f"HOSTNAME={config.database.db2_host};"
        f"PORT={config.database.db2_port};"
        f"PROTOCOL=TCPIP;"
        f"UID={config.database.db2_user};"
        f"CURRENTSCHEMA={config.database.db2_schema};"
    )
    
    for i, password in enumerate(password_variations, 1):
        logger.info(f"\n🔐 Test {i:2d}: Trying password '{password}'...")
        
        try:
            conn_str = base_conn_str + f"PWD={password};"
            conn = pyodbc.connect(conn_str, timeout=10)
            
            # Test with a simple query
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1")
            result = cursor.fetchone()
            
            conn.close()
            
            logger.info(f"✅ SUCCESS! Password '{password}' works!")
            logger.info(f"   Server time: {result[0]}")
            
            # Update .env file with working password
            update_env_password(password)
            return password
            
        except pyodbc.Error as e:
            error_msg = str(e)
            if "PASSWORD EXPIRED" in error_msg:
                logger.info(f"❌ Password '{password}' is expired")
            elif "INVALID AUTHORIZATION" in error_msg or "SQL30082N" in error_msg:
                logger.info(f"❌ Password '{password}' is invalid")
            elif "timeout" in error_msg.lower():
                logger.info(f"⏱️ Password '{password}' - connection timeout")
            else:
                logger.info(f"❌ Password '{password}' failed: {error_msg[:100]}...")
        
        except Exception as e:
            logger.info(f"❌ Password '{password}' error: {str(e)[:100]}...")
    
    logger.info("\n" + "=" * 50)
    logger.info("❌ No working password found!")
    logger.info("💡 You need to contact your DB2 administrator to reset the password.")
    return None

def update_env_password(new_password):
    """Update .env file with working password"""
    logger = logging.getLogger(__name__)
    
    try:
        # Read current .env file
        with open('.env', 'r') as f:
            lines = f.readlines()
        
        # Update password line
        updated_lines = []
        for line in lines:
            if line.startswith('DB2_PASSWORD='):
                updated_lines.append(f'DB2_PASSWORD={new_password}\n')
                logger.info(f"📝 Updated .env file with new password")
            else:
                updated_lines.append(line)
        
        # Write back to .env file
        with open('.env', 'w') as f:
            f.writelines(updated_lines)
            
    except Exception as e:
        logger.error(f"❌ Failed to update .env file: {e}")

if __name__ == "__main__":
    working_password = test_password_variations()
    
    if working_password:
        print(f"\n🎉 Found working password: {working_password}")
        print("✅ .env file updated - you can now run the cash pipeline!")
    else:
        print("\n💼 Next steps:")
        print("1. Contact your DB2 administrator")
        print("2. Ask them to reset password for user 'profits'")
        print("3. Update the DB2_PASSWORD in .env file")
        print("4. Run the cash pipeline test again")