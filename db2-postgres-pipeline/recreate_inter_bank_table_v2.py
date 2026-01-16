"""
Recreate interBankLoanReceivable table with V2 schema
"""
import psycopg2
from config import DatabaseConfig

db_config = DatabaseConfig()

pg_config = {
    'host': db_config.pg_host,
    'port': db_config.pg_port,
    'database': db_config.pg_database,
    'user': db_config.pg_user,
    'password': db_config.pg_password
}

try:
    pg_conn = psycopg2.connect(**pg_config)
    pg_cursor = pg_conn.cursor()
    
    print("=" * 60)
    print("RECREATING interBankLoanReceivable TABLE WITH V2 SCHEMA")
    print("=" * 60)
    
    # Drop the old table
    print("\n1. Dropping old table...")
    drop_query = 'DROP TABLE IF EXISTS "interBankLoanReceivable" CASCADE'
    pg_cursor.execute(drop_query)
    pg_conn.commit()
    print("✓ Old table dropped")
    
    # Create new table with V2 schema
    print("\n2. Creating new table with V2 schema...")
    create_query = """
    CREATE TABLE "interBankLoanReceivable" (
        id SERIAL PRIMARY KEY,
        "reportingDate" TIMESTAMP,
        "borrowersInstitutionCode" VARCHAR(255),
        "borrowerCountry" VARCHAR(255),
        "relationshipType" VARCHAR(255),
        "ratingStatus" SMALLINT,
        "externalRatingCorrespondentBorrower" VARCHAR(255),
        "gradesUnratedBorrower" VARCHAR(255),
        "loanNumber" VARCHAR(255) UNIQUE,
        "loanType" VARCHAR(255),
        "issueDate" DATE,
        "loanMaturityDate" DATE,
        "currency" VARCHAR(10),
        "orgLoanAmount" NUMERIC(15, 2),
        "usdLoanAmount" NUMERIC(15, 2),
        "tzsLoanAmount" NUMERIC(15, 2),
        "interestRate" NUMERIC(10, 4),
        "orgAccruedInterestAmount" NUMERIC(15, 2),
        "usdAccruedInterestAmount" NUMERIC(15, 2),
        "tzsAccruedInterestAmount" NUMERIC(15, 2),
        "orgSuspendedInterest" NUMERIC(15, 2),
        "usdSuspendedInterest" NUMERIC(15, 2),
        "tzsSuspendedInterest" NUMERIC(15, 2),
        "pastDueDays" INTEGER,
        "allowanceProbableLoss" NUMERIC(15, 2),
        "botProvision" NUMERIC(15, 2),
        "assetClassificationCategory" VARCHAR(255)
    )
    """
    pg_cursor.execute(create_query)
    pg_conn.commit()
    print("✓ New table created")
    
    # Create indexes
    print("\n3. Creating indexes...")
    indexes = [
        'CREATE INDEX idx_inter_bank_loan_number ON "interBankLoanReceivable"("loanNumber")',
        'CREATE INDEX idx_inter_bank_institution ON "interBankLoanReceivable"("borrowersInstitutionCode")',
        'CREATE INDEX idx_inter_bank_issue_date ON "interBankLoanReceivable"("issueDate")',
        'CREATE INDEX idx_inter_bank_currency ON "interBankLoanReceivable"("currency")'
    ]
    
    for idx_query in indexes:
        pg_cursor.execute(idx_query)
    
    pg_conn.commit()
    print("✓ Indexes created")
    
    # Verify
    print("\n4. Verifying new table...")
    verify_query = """
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name = 'interBankLoanReceivable'
    ORDER BY ordinal_position
    """
    pg_cursor.execute(verify_query)
    columns = pg_cursor.fetchall()
    
    print(f"\n✓ Table recreated successfully with {len(columns)} columns:")
    for col_name, col_type in columns:
        print(f"  - {col_name:40} {col_type}")
    
    print("\n" + "=" * 60)
    print("TABLE RECREATION COMPLETE!")
    print("=" * 60)
    
    pg_cursor.close()
    pg_conn.close()
    
except Exception as e:
    print(f"\n❌ Error: {e}")
    if 'pg_conn' in locals():
        pg_conn.rollback()
        pg_conn.close()
