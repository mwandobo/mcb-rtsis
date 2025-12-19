#!/usr/bin/env python3
"""
Debug Branch Data - Check what's coming from DB2
"""

from db2_connection import DB2Connection
from config import Config
import logging

def debug_branch_data():
    """Debug what data is coming from DB2"""
    config = Config()
    db2_conn = DB2Connection()
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    try:
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get the branch query
            branch_config = config.tables['branch']
            logger.info("🔍 Executing branch query to debug data...")
            
            cursor.execute(branch_config.query)
            
            rows = cursor.fetchall()
            logger.info(f"📊 Found {len(rows)} rows")
            
            for i, row in enumerate(rows):
                logger.info(f"\n📋 Row {i+1}:")
                logger.info(f"  0. reportingDate: '{row[0]}'")
                logger.info(f"  1. branchName: '{row[1]}'")
                logger.info(f"  2. taxIdentificationNumber: '{row[2]}'")
                logger.info(f"  3. businessLicense: '{row[3]}'")
                logger.info(f"  4. branchCode: '{row[4]}'")
                logger.info(f"  5. qrFsrCode: '{row[5]}'")
                logger.info(f"  6. region: '{row[6]}'")
                logger.info(f"  7. district: '{row[7]}'")
                logger.info(f"  8. ward: '{row[8]}'")
                logger.info(f"  9. street: '{row[9]}'")
                logger.info(f" 10. houseNumber: '{row[10]}'")
                logger.info(f" 11. postalCode: '{row[11]}'")
                logger.info(f" 12. gpsCoordinates: '{row[12]}'")
                logger.info(f" 13. bankingServices: '{row[13]}'")
                logger.info(f" 14. mobileMoneyServices: '{row[14]}'")
                logger.info(f" 15. registrationDate: '{row[15]}'")
                logger.info(f" 16. branchStatus: '{row[16]}'")
                logger.info(f" 17. closureDate: '{row[17]}'")
                logger.info(f" 18. contactPerson: '{row[18]}'")
                logger.info(f" 19. telephoneNumber: '{row[19]}'")
                logger.info(f" 20. altTelephoneNumber: '{row[20]}'")
                logger.info(f" 21. branchCategory: '{row[21]}'")
                logger.info(f" 22. lastModified: '{row[22]}'")
                
                # Check validation criteria
                logger.info(f"\n🔍 Validation Check for Row {i+1}:")
                logger.info(f"  branchCode empty? {not str(row[4])}")
                logger.info(f"  branchName empty? {not str(row[1])}")
                logger.info(f"  branchStatus empty? {not str(row[16])}")
                logger.info(f"  region empty? {not str(row[6])}")
                
    except Exception as e:
        logger.error(f"❌ Debug error: {e}")

if __name__ == "__main__":
    debug_branch_data()