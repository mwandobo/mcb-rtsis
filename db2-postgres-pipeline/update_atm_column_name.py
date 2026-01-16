"""
Update atmTransaction table column name from transactionNature to transactionType
"""
import psycopg2
from config import DatabaseConfig

def update_column_name():
    """Rename transactionNature column to transactionType"""
    db_config = DatabaseConfig()
    
    pg_config = {
        'host': db_config.pg_host,
        'port': db_config.pg_port,
        'database': db_config.pg_database,
        'user': db_config.pg_user,
        'password': db_config.pg_password
    }
    
    try:
        # Connect to PostgreSQL
        pg_conn = psycopg2.connect(**pg_config)
        pg_cursor = pg_conn.cursor()
        
        print("Connected to PostgreSQL")
        
        # Rename the column
        alter_query = """
        ALTER TABLE "atmTransaction" 
        RENAME COLUMN "transactionNature" TO "transactionType"
        """
        
        print("Renaming column transactionNature to transactionType...")
        pg_cursor.execute(alter_query)
        pg_conn.commit()
        
        print("✓ Column renamed successfully!")
        
        # Verify the change
        verify_query = """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'atmTransaction' 
        AND column_name IN ('transactionNature', 'transactionType')
        """
        
        pg_cursor.execute(verify_query)
        columns = pg_cursor.fetchall()
        
        print("\nCurrent columns:")
        for col in columns:
            print(f"  - {col[0]}")
        
        pg_cursor.close()
        pg_conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
        if 'pg_conn' in locals():
            pg_conn.rollback()
            pg_conn.close()

if __name__ == "__main__":
    update_column_name()
