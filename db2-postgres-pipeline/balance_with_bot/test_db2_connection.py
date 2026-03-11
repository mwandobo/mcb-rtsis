import ibm_db
import os
from pathlib import Path

def load_environment():
    """Load environment variables from .env file"""
    env_path = Path('../.env')
    if env_path.exists():
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()

def test_db2_connection():
    load_environment()
    
    db2_host = os.getenv('DB2_HOST', 'localhost')
    db2_port = os.getenv('DB2_PORT', '50000')
    db2_database = os.getenv('DB2_DATABASE', 'BANKDB')
    db2_username = os.getenv('DB2_USERNAME', 'db2inst1')
    db2_password = os.getenv('DB2_PASSWORD', 'password')
    
    conn_str = f"DATABASE={db2_database};HOSTNAME={db2_host};PORT={db2_port};PROTOCOL=TCPIP;UID={db2_username};PWD={db2_password};"
    print(f"Connecting to: {db2_host}:{db2_port}/{db2_database}")
    
    try:
        conn = ibm_db.connect(conn_str, "", "")
        print("✅ Connected successfully to DB2")
        
        # List all tables in DB2INST1 schema
        print("\n📋 Checking available tables...")
        stmt = ibm_db.exec_immediate(conn, "SELECT TABNAME FROM SYSCAT.TABLES WHERE TABSCHEMA = 'DB2INST1' ORDER BY TABNAME")
        if stmt:
            tables = []
            row = ibm_db.fetch_assoc(stmt)
            while row:
                tables.append(row['TABNAME'])
                row = ibm_db.fetch_assoc(stmt)
            
            if tables:
                print(f"Found {len(tables)} tables in DB2INST1 schema:")
                for table in tables:
                    print(f"  - {table}")
            else:
                print("No tables found in DB2INST1 schema")
        
        # Check if BALANCE_WITH_BOT table exists
        print("\n🔍 Checking for BALANCE_WITH_BOT table...")
        stmt = ibm_db.exec_immediate(conn, "SELECT COUNT(*) as cnt FROM SYSCAT.TABLES WHERE TABSCHEMA = 'DB2INST1' AND TABNAME = 'BALANCE_WITH_BOT'")
        if stmt:
            row = ibm_db.fetch_assoc(stmt)
            if row and row['CNT'] > 0:
                print("✅ BALANCE_WITH_BOT table exists")
                
                # Get record count
                stmt2 = ibm_db.exec_immediate(conn, "SELECT COUNT(*) as total FROM DB2INST1.BALANCE_WITH_BOT")
                if stmt2:
                    row2 = ibm_db.fetch_assoc(stmt2)
                    print(f"📊 Table has {row2['TOTAL']} records")
                    
                    # Show sample data
                    if row2['TOTAL'] > 0:
                        print("\n📄 Sample data (first 3 rows):")
                        stmt3 = ibm_db.exec_immediate(conn, "SELECT * FROM DB2INST1.BALANCE_WITH_BOT FETCH FIRST 3 ROWS ONLY")
                        if stmt3:
                            sample_row = ibm_db.fetch_assoc(stmt3)
                            row_count = 0
                            while sample_row and row_count < 3:
                                print(f"Row {row_count + 1}: {sample_row}")
                                sample_row = ibm_db.fetch_assoc(stmt3)
                                row_count += 1
            else:
                print("❌ BALANCE_WITH_BOT table does not exist")
                print("🔧 Creating BALANCE_WITH_BOT table...")
                create_table()
        
        ibm_db.close(conn)
        
    except Exception as e:
        print(f"❌ Error: {e}")

def create_table():
    """Create the BALANCE_WITH_BOT table in DB2"""
    load_environment()
    
    db2_host = os.getenv('DB2_HOST', 'localhost')
    db2_port = os.getenv('DB2_PORT', '50000')
    db2_database = os.getenv('DB2_DATABASE', 'BANKDB')
    db2_username = os.getenv('DB2_USERNAME', 'db2inst1')
    db2_password = os.getenv('DB2_PASSWORD', 'password')
    
    conn_str = f"DATABASE={db2_database};HOSTNAME={db2_host};PORT={db2_port};PROTOCOL=TCPIP;UID={db2_username};PWD={db2_password};"
    
    try:
        conn = ibm_db.connect(conn_str, "", "")
        
        create_sql = """
        CREATE TABLE DB2INST1.BALANCE_WITH_BOT (
            ACCOUNT_NUMBER VARCHAR(50) NOT NULL,
            BALANCE_AMOUNT DECIMAL(15,2),
            BALANCE_DATE TIMESTAMP,
            ACCOUNT_TYPE VARCHAR(20),
            BRANCH_CODE VARCHAR(10),
            CURRENCY_CODE VARCHAR(3),
            LAST_TRANSACTION_DATE TIMESTAMP,
            ACCOUNT_STATUS VARCHAR(20),
            AVAILABLE_BALANCE DECIMAL(15,2),
            PENDING_BALANCE DECIMAL(15,2),
            OVERDRAFT_LIMIT DECIMAL(15,2),
            INTEREST_RATE DECIMAL(5,4),
            PRIMARY KEY (ACCOUNT_NUMBER)
        )
        """
        
        stmt = ibm_db.exec_immediate(conn, create_sql)
        if stmt:
            print("✅ BALANCE_WITH_BOT table created successfully")
            
            # Insert some sample data
            print("📝 Inserting sample data...")
            sample_data = [
                ("ACC001", 1500.50, "2024-01-15 10:30:00", "SAVINGS", "BR001", "USD", "2024-01-14 15:20:00", "ACTIVE", 1500.50, 0.00, 0.00, 0.0250),
                ("ACC002", 2750.75, "2024-01-15 11:45:00", "CHECKING", "BR002", "USD", "2024-01-15 09:15:00", "ACTIVE", 2750.75, 0.00, 500.00, 0.0150),
                ("ACC003", 850.25, "2024-01-15 14:20:00", "SAVINGS", "BR001", "USD", "2024-01-13 16:30:00", "ACTIVE", 850.25, 0.00, 0.00, 0.0300)
            ]
            
            for data in sample_data:
                insert_sql = """
                INSERT INTO DB2INST1.BALANCE_WITH_BOT 
                (ACCOUNT_NUMBER, BALANCE_AMOUNT, BALANCE_DATE, ACCOUNT_TYPE, BRANCH_CODE, 
                 CURRENCY_CODE, LAST_TRANSACTION_DATE, ACCOUNT_STATUS, AVAILABLE_BALANCE, 
                 PENDING_BALANCE, OVERDRAFT_LIMIT, INTEREST_RATE)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                stmt = ibm_db.prepare(conn, insert_sql)
                if stmt:
                    ibm_db.bind_param(stmt, 1, data[0])
                    ibm_db.bind_param(stmt, 2, data[1])
                    ibm_db.bind_param(stmt, 3, data[2])
                    ibm_db.bind_param(stmt, 4, data[3])
                    ibm_db.bind_param(stmt, 5, data[4])
                    ibm_db.bind_param(stmt, 6, data[5])
                    ibm_db.bind_param(stmt, 7, data[6])
                    ibm_db.bind_param(stmt, 8, data[7])
                    ibm_db.bind_param(stmt, 9, data[8])
                    ibm_db.bind_param(stmt, 10, data[9])
                    ibm_db.bind_param(stmt, 11, data[10])
                    ibm_db.bind_param(stmt, 12, data[11])
                    
                    result = ibm_db.execute(stmt)
                    if result:
                        print(f"  ✅ Inserted record for {data[0]}")
                    else:
                        print(f"  ❌ Failed to insert record for {data[0]}")
            
            print("✅ Sample data inserted successfully")
        else:
            print(f"❌ Failed to create table: {ibm_db.stmt_errormsg()}")
        
        ibm_db.close(conn)
        
    except Exception as e:
        print(f"❌ Error creating table: {e}")

if __name__ == "__main__":
    test_db2_connection()