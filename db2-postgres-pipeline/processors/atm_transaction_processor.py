"""
ATM transaction processor - Based on atm_transaction.sql
"""

from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime
from .base import BaseProcessor, BaseRecord

@dataclass
class AtmTransactionRecord(BaseRecord):
    """ATM transaction record structure - Based on atm_transaction.sql"""
    reporting_date: str
    atm_code: str
    transaction_date: str
    transaction_id: str
    transaction_type: str
    currency: str
    org_transaction_amount: Optional[float]
    tzs_transaction_amount: Optional[float]
    atm_channel: str
    value_added_tax_amount: Optional[float]
    excise_duty_amount: Optional[float]
    electronic_levy_amount: Optional[float]
    retry_count: int = 0

class AtmTransactionProcessor(BaseProcessor):
    """Processor for ATM transaction data - Based on atm_transaction.sql"""
    
    def process_record(self, raw_data: Tuple, table_name: str) -> AtmTransactionRecord:
        """Convert raw DB2 data to AtmTransactionRecord"""
        # Handle None values safely
        def safe_str(value):
            return str(value).strip() if value is not None else None
        
        def safe_float(value):
            try:
                return float(value) if value is not None else None
            except (ValueError, TypeError):
                return None
        
        return AtmTransactionRecord(
            source_table=table_name,
            timestamp_column_value=safe_str(raw_data[2]),  # transactionDate for tracking
            reporting_date=safe_str(raw_data[0]),
            atm_code=safe_str(raw_data[1]),
            transaction_date=safe_str(raw_data[2]),
            transaction_id=safe_str(raw_data[3]),
            transaction_type=safe_str(raw_data[4]),
            currency=safe_str(raw_data[5]),
            org_transaction_amount=safe_float(raw_data[6]),
            tzs_transaction_amount=safe_float(raw_data[7]),
            atm_channel=safe_str(raw_data[8]),
            value_added_tax_amount=safe_float(raw_data[9]),
            excise_duty_amount=safe_float(raw_data[10]),
            electronic_levy_amount=safe_float(raw_data[11]),
            original_timestamp=datetime.now().isoformat()
        )
    
    def insert_to_postgres(self, record: AtmTransactionRecord, pg_cursor) -> None:
        """Insert ATM transaction record to PostgreSQL"""
        query = self.get_upsert_query()
        
        pg_cursor.execute(query, (
            record.reporting_date,
            record.atm_code,
            record.transaction_date,
            record.transaction_id,
            record.transaction_type,
            record.currency,
            record.org_transaction_amount,
            record.tzs_transaction_amount,
            record.atm_channel,
            record.value_added_tax_amount,
            record.excise_duty_amount,
            record.electronic_levy_amount
        ))
    
    def get_upsert_query(self) -> str:
        """Get upsert query for ATM transaction with duplicate handling"""
        return """
        INSERT INTO "atmTransaction" (
            "reportingDate", "atmCode", "transactionDate", "transactionId", "transactionType",
            "currency", "orgTransactionAmount", "tzsTransactionAmount", "atmChannel",
            "valueAddedTaxAmount", "exciseDutyAmount", "electronicLevyAmount"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT ("transactionId") DO UPDATE SET
            "reportingDate" = EXCLUDED."reportingDate",
            "atmCode" = EXCLUDED."atmCode",
            "transactionDate" = EXCLUDED."transactionDate",
            "transactionType" = EXCLUDED."transactionType",
            "currency" = EXCLUDED."currency",
            "orgTransactionAmount" = EXCLUDED."orgTransactionAmount",
            "tzsTransactionAmount" = EXCLUDED."tzsTransactionAmount",
            "atmChannel" = EXCLUDED."atmChannel",
            "valueAddedTaxAmount" = EXCLUDED."valueAddedTaxAmount",
            "exciseDutyAmount" = EXCLUDED."exciseDutyAmount",
            "electronicLevyAmount" = EXCLUDED."electronicLevyAmount"
        """
    
    def validate_record(self, record: AtmTransactionRecord) -> bool:
        """Validate ATM transaction record"""
        if not super().validate_record(record):
            return False
        
        # ATM transaction-specific validations
        if not record.atm_code:
            return False
        if not record.transaction_id:
            return False
        if not record.transaction_date:
            return False
        if not record.transaction_type:
            return False
        if not record.currency:
            return False
            
        return True
    
    def transform_data(self, raw_data: Tuple) -> dict:
        """Transform ATM transaction data"""
        # Add any ATM transaction-specific transformations here
        return {
            'atm_code_normalized': str(raw_data[1]).strip().upper() if raw_data[1] else '',
            'currency_normalized': str(raw_data[5]).strip().upper() if raw_data[5] else 'TZS',
            'transaction_nature_normalized': str(raw_data[4]).strip().title() if raw_data[4] else 'Others'
        }