import psycopg2
import logging
from config import Config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def execute_update():
    """Execute the update SQL to set standard values for balance with other banks"""
    try:
        config = Config()
        
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        cursor = conn.cursor()
        
        logging.info("Connected to PostgreSQL")
        
        # Check current state
        cursor.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT "subAccountType") as distinct_subAccountType,
                COUNT(DISTINCT "externalRatingCorrespondentBank") as distinct_externalRating,
                COUNT(DISTINCT "gradesUnratedBanks") as distinct_grades
            FROM "balanceWithOtherBank";
        """)
        
        before = cursor.fetchone()
        logging.info(f"BEFORE UPDATE:")
        logging.info(f"  Total records: {before[0]}")
        logging.info(f"  Distinct subAccountType values: {before[1]}")
        logging.info(f"  Distinct externalRatingCorrespondentBank values: {before[2]}")
        logging.info(f"  Distinct gradesUnratedBanks values: {before[3]}")
        
        # Execute update
        logging.info("Executing UPDATE statement...")
        cursor.execute("""
            UPDATE "balanceWithOtherBank"
            SET 
                "subAccountType" = 'Normal',
                "externalRatingCorrespondentBank" = 'Unrated',
                "gradesUnratedBanks" = 'Grade B';
        """)
        
        rows_updated = cursor.rowcount
        logging.info(f"Updated {rows_updated} records")
        
        # Commit the transaction
        conn.commit()
        logging.info("Changes committed successfully")
        
        # Check after state
        cursor.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT "subAccountType") as distinct_subAccountType,
                COUNT(DISTINCT "externalRatingCorrespondentBank") as distinct_externalRating,
                COUNT(DISTINCT "gradesUnratedBanks") as distinct_grades
            FROM "balanceWithOtherBank";
        """)
        
        after = cursor.fetchone()
        logging.info(f"AFTER UPDATE:")
        logging.info(f"  Total records: {after[0]}")
        logging.info(f"  Distinct subAccountType values: {after[1]}")
        logging.info(f"  Distinct externalRatingCorrespondentBank values: {after[2]}")
        logging.info(f"  Distinct gradesUnratedBanks values: {after[3]}")
        
        # Show sample records
        cursor.execute("""
            SELECT 
                "accountNumber",
                "subAccountType",
                "externalRatingCorrespondentBank",
                "gradesUnratedBanks"
            FROM "balanceWithOtherBank"
            LIMIT 5;
        """)
        
        logging.info("\nSample records after update:")
        for row in cursor.fetchall():
            logging.info(f"  Account: {row[0]}, SubType: {row[1]}, Rating: {row[2]}, Grade: {row[3]}")
        
        cursor.close()
        conn.close()
        logging.info("\nUpdate completed successfully!")
        
    except Exception as e:
        logging.error(f"Error executing update: {e}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        raise

if __name__ == "__main__":
    execute_update()
