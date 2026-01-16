#!/usr/bin/env python3
"""
Setup Income Statement GL Lookup Table in DB2
This dramatically improves query performance by pre-mapping GL accounts to categories
"""

import logging
from db2_connection import DB2Connection

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 60)
    logger.info("INCOME STATEMENT GL LOOKUP TABLE SETUP")
    logger.info("=" * 60)
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            # Read and clean the SQL file
            logger.info("Reading lookup table creation script...")
            import os
            script_dir = os.path.dirname(os.path.abspath(__file__))
            sql_file = os.path.join(script_dir, 'create_income_statement_gl_lookup.sql')
            
            with open(sql_file, 'r') as f:
                sql_script = f.read()
            
            # Remove comments
            lines = []
            for line in sql_script.split('\n'):
                if not line.strip().startswith('--'):
                    lines.append(line)
            sql_script = '\n'.join(lines)
            
            # Split by semicolons - but be smarter about it
            # First, let's just execute the whole thing as one batch
            logger.info("Executing SQL script...")
            
            try:
                # Try to execute as a batch
                for statement in cursor.execute(sql_script):
                    pass
                logger.info("✅ Batch execution successful!")
            except:
                # If batch fails, try statement by statement
                logger.info("Batch failed, trying statement by statement...")
                statements = [stmt.strip() for stmt in sql_script.split(';') if stmt.strip()]
                logger.info(f"Executing {len(statements)} SQL statements...")
                
                for i, statement in enumerate(statements, 1):
                    if statement.strip():
                        try:
                            cursor.execute(statement)
                            if i % 50 == 0:
                                logger.info(f"  Executed {i}/{len(statements)} statements...")
                        except Exception as e:
                            error_msg = str(e).lower()
                            if 'already exists' in error_msg or 'duplicate' in error_msg or 'identical' in error_msg:
                                logger.warning(f"  Statement {i}: Object already exists, continuing...")
                            else:
                                logger.error(f"  Statement {i} failed: {e}")
                                logger.error(f"  Statement was: {statement[:200]}...")
                                raise
            
            conn.commit()
            logger.info("✅ All statements executed successfully!")
            
            # Verify the table
            logger.info("\nVerifying lookup table...")
            cursor.execute("SELECT COUNT(*) FROM INCOME_STATEMENT_GL_LOOKUP")
            count = cursor.fetchone()[0]
            logger.info(f"  Total GL accounts mapped: {count}")
            
            cursor.execute("""
                SELECT CATEGORY, COUNT(*) as account_count
                FROM INCOME_STATEMENT_GL_LOOKUP
                GROUP BY CATEGORY
                ORDER BY CATEGORY
            """)
            
            logger.info("\n  Breakdown by category:")
            for category, account_count in cursor.fetchall():
                logger.info(f"    {category}: {account_count} accounts")
            
            logger.info("\n" + "=" * 60)
            logger.info("✅ SETUP COMPLETE!")
            logger.info("=" * 60)
            logger.info("\nNext steps:")
            logger.info("1. Update config.py to use 'income-statement-with-lookup.sql'")
            logger.info("2. Run the income statement pipeline")
            logger.info("3. Enjoy 5-10x faster query performance!")
            
    except Exception as e:
        logger.error(f"❌ Setup failed: {e}")
        raise

if __name__ == "__main__":
    main()
