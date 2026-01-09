"""
ICBM transaction processor - Based on icbm_transactions.sql
"""

from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime
from .base import BaseProcessor, BaseRecord

@dataclass
class IcbmTransactionRecord(BaseRecord):
    """ICBM transaction record structure"""
    reporting_date: str
    transaction_date: str
    lender_name: Optional[str]
    borrower_name: Optional[str]
    transaction_type: Optional[str]
    tzs_amount: Optional[float]
    tenure: Optional[str]
    interest_rate: Optional[str]
    retry_count: int = 0

class IcbmTransactionProcessor(BaseProcessor):
    """Processor for ICBM transaction data"""
    
    def process_record(self, raw_data: Tuple, table_name: str) -> IcbmTransactionRecord:
        """Convert raw DB2 data to IcbmTransactionRecord"""
        # Handle None values safely
        def safe_str(value):
            return str(value).strip() if value is not None else None
        
        def safe_float(value):
            try:
                return float(value) if value is not None else None
            except (ValueError, TypeError):
                return None
        
        return IcbmTransactionRecord(
            source_table=table_name,
            timestamp_column_value=safe_str(raw_data[1]),  # transactionDate for tracking
            reporting_date=safe_str(raw_data[0]),
            transaction_date=safe_str(raw_data[1]),
            lender_name=safe_str(raw_data[2]),
            borrower_name=safe_str(raw_data[3]),
            transaction_type=safe_str(raw_data[4]),
            tzs_amount=safe_float(raw_data[5]),
            tenure=safe_str(raw_data[6]),
            interest_rate=safe_str(raw_data[7]),
            original_timestamp=datetime.now().isoformat()
        )
    
    def insert_to_postgres(self, record: IcbmTransactionRecord, pg_cursor) -> None:
        """Insert ICBM transaction record to PostgreSQL"""
        query = self.get_insert_query()
        
        pg_cursor.execute(query, (
            record.reporting_date,
            record.transaction_date,
            record.lender_name,
            record.borrower_name,
            record.transaction_type,
            record.tzs_amount,
            record.tenure,
            record.interest_rate
        ))
    
    def get_insert_query(self) -> str:
        """Get insert query for ICBM transaction"""
        return """
        INSERT INTO "icbmTransaction" (
            "reportingDate", "transactionDate", "lenderName", "borrowerName",
            "transactionType", "tzsAmount", "tenure", "interestRate"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
    
    def get_upsert_query(self) -> str:
        """Get upsert query for ICBM transaction (use transaction date and amount as unique key)"""
        return """
        INSERT INTO "icbmTransaction" (
            "reportingDate", "transactionDate", "lenderName", "borrowerName",
            "transactionType", "tzsAmount", "tenure", "interestRate"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT ("transactionDate", "tzsAmount") DO UPDATE SET
            "reportingDate" = EXCLUDED."reportingDate",
            "lenderName" = EXCLUDED."lenderName",
            "borrowerName" = EXCLUDED."borrowerName",
            "transactionType" = EXCLUDED."transactionType",
            "tenure" = EXCLUDED."tenure",
            "interestRate" = EXCLUDED."interestRate"
        """
    
    def validate_record(self, record: IcbmTransactionRecord) -> bool:
        """Validate ICBM transaction record"""
        if not super().validate_record(record):
            return False
        
        # ICBM transaction-specific validations
        if not record.transaction_date:
            return False
        if record.tzs_amount is None or record.tzs_amount <= 0:
            return False
        if not record.transaction_type:
            return False
            
        return True
    
    def transform_data(self, raw_data: Tuple) -> dict:
        """Transform ICBM transaction data"""
        # Add any ICBM transaction-specific transformations here
        return {
            'transaction_type_normalized': str(raw_data[4]).strip().lower() if raw_data[4] else 'unknown',
            'amount_millions': (raw_data[5] / 1000000) if raw_data[5] else 0,
            'is_market_hours': str(raw_data[4]).strip().lower() == 'market' if raw_data[4] else False
        }