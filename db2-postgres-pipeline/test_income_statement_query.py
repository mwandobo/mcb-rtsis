#!/usr/bin/env python3
"""
Test the optimized income statement query with lookup table
"""

import logging
import time
from db2_connection import DB2Connection

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 60)
    logger.info("TESTING OPTIMIZED INCOME STATEMENT QUERY")
    logger.info("=" * 60)
    
    db2_conn = DB2Connection()
    
    try:
        # Read the optimized query
        import os
        script_dir = os.path.dirname(os.path.abspath(__file__))
        query_file = os.path.join(os.path.dirname(script_dir), 'sqls', 'income-statement-with-lookup-v2.sql')
        
        logger.info(f"Reading query from: {query_file}")
        with open(query_file, 'r') as f:
            query = f.read()
        
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            logger.info("Executing optimized query...")
            start_time = time.time()
            
            cursor.execute(query)
            result = cursor.fetchone()
            
            elapsed = time.time() - start_time
            
            logger.info(f"✅ Query executed successfully in {elapsed:.2f} seconds")
            logger.info("")
            logger.info("Results:")
            logger.info(f"  reportingDate: {result[0]}")
            logger.info(f"  interestIncome: {result[1]}")
            logger.info(f"  interestIncomeValue: {result[2]}")
            logger.info(f"  interestExpenses: {result[3]}")
            logger.info(f"  interestExpensesValue: {result[4]}")
            logger.info(f"  badDebtsWrittenOffNotProvided: {result[5]}")
            logger.info(f"  provisionBadDoubtfulDebts: {result[6]}")
            logger.info(f"  impairmentsInvestments: {result[7]}")
            logger.info(f"  incomeTaxProvision: {result[8]}")
            logger.info(f"  extraordinaryCreditsCharge: {result[9]}")
            logger.info(f"  nonCoreCreditsCharges: {result[10]}")
            logger.info(f"  nonCoreCreditsChargesValue: {result[11]}")
            logger.info(f"  nonInterestIncome: {result[12]}")
            logger.info(f"  nonInterestIncomeValue: {result[13]}")
            logger.info(f"  nonInterestExpenses: {result[14]}")
            logger.info(f"  nonInterestExpensesValue: {result[15]}")
            
            logger.info("")
            logger.info("=" * 60)
            logger.info("✅ TEST PASSED!")
            logger.info(f"Query performance: {elapsed:.2f} seconds")
            logger.info("=" * 60)
            
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()
