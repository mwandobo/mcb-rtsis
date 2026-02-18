#!/usr/bin/env python3
"""
Create posInformation table
"""

import psycopg2
import logging
from config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_pos_table():
    """Create posInformation table"""
    config = Config()

    try:
        conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password,
        )

        cursor = conn.cursor()

        # Check if table exists
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'posInformation'
            )
        """
        )

        table_exists = cursor.fetchone()[0]

        if table_exists:
            logger.info("Table 'posInformation' already exists")
        else:
            logger.info("Creating 'posInformation' table...")

            create_table_sql = """
            CREATE TABLE "posInformation" (
                id SERIAL PRIMARY KEY,
                "reportingDate" VARCHAR(50),
                "posBranchCode" INTEGER,
                "posNumber" VARCHAR(50) UNIQUE NOT NULL,
                "qrFsrCode" VARCHAR(50),
                "posHolderCategory" VARCHAR(50),
                "posHolderName" VARCHAR(255),
                "posHolderNin" VARCHAR(50),
                "posHolderTin" VARCHAR(50),
                "postalCode" VARCHAR(20),
                region VARCHAR(100),
                district VARCHAR(100),
                ward VARCHAR(100),
                street VARCHAR(255),
                "houseNumber" VARCHAR(50),
                "gpsCoordinates" VARCHAR(100),
                "linkedAccount" VARCHAR(50),
                "issueDate" VARCHAR(50),
                "returnDate" VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """

            cursor.execute(create_table_sql)
            logger.info("✓ Table created")

            # Create indexes
            logger.info("Creating indexes...")
            cursor.execute(
                'CREATE INDEX idx_pos_pos_number ON "posInformation"("posNumber")'
            )
            cursor.execute(
                'CREATE INDEX idx_pos_region ON "posInformation"(region)'
            )
            cursor.execute(
                'CREATE INDEX idx_pos_district ON "posInformation"(district)'
            )
            logger.info("✓ Indexes created")

            # Create trigger for updated_at
            logger.info("Creating trigger for updated_at...")
            cursor.execute(
                """
                CREATE TRIGGER update_posinformation_updated_at
                BEFORE UPDATE ON "posInformation"
                FOR EACH ROW
                EXECUTE FUNCTION update_updated_at_column();
            """
            )
            logger.info("✓ Trigger created")

            conn.commit()

        # Show table structure
        cursor.execute(
            """
            SELECT column_name, data_type, character_maximum_length, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'posInformation'
            ORDER BY ordinal_position
        """
        )

        columns = cursor.fetchall()
        logger.info("\n" + "=" * 80)
        logger.info("POS INFORMATION TABLE STRUCTURE")
        logger.info("=" * 80)
        for col_name, data_type, max_length, nullable in columns:
            length_info = f"({max_length})" if max_length else ""
            null_info = "NULL" if nullable == "YES" else "NOT NULL"
            logger.info(f"  {col_name:30} {data_type}{length_info:20} {null_info}")
        logger.info("=" * 80)

        cursor.close()
        conn.close()

        logger.info("\n✓ POS table ready for pipeline execution")

    except Exception as e:
        logger.error(f"Error: {e}")
        raise


if __name__ == "__main__":
    logger.info("=" * 80)
    logger.info("CREATING POS INFORMATION TABLE")
    logger.info("=" * 80)
    create_pos_table()
    logger.info("=" * 80)
    logger.info("DONE")
    logger.info("=" * 80)
