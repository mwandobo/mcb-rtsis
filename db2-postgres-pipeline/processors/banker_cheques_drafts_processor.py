"""
Banker cheques and drafts processor - Based on banker-cheques-and-draft-lss.sql
"""

from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime
from .base import BaseProcessor, BaseRecord

@dataclass
class BankerChequesDraftsRecord(BaseRecord):
    """Banker cheques and drafts record structure"""
    reporting_date: str
    customer_identification_number: str
    customer_name: Optional[str]
    beneficiary_name: Optional[str]
    check_number: Optional[str]
    transaction_date: str
    value_date: str
    maturity_date: Optional[str]
    currency: str
    org_amount: Optional[float]
    usd_amount: Optional[float]
    tzs_amount: Optional[float]
    retry_count: int = 0

class BankerChequesDraftsProcessor(BaseProcessor):
    """Processor for banker cheques and drafts data"""
    
    def process_record(self, raw_data: Tuple, table_name: str) -> BankerChequesDraftsRecord:
        """Convert raw DB2 data to BankerChequesDraftsRecord"""
        # Handle None values safely
        def safe_str(value):
            return str(value).strip() if value is not None else None
        
        def safe_float(value):
            try:
                return float(value) if value is not None else None
            except (ValueError, TypeError):
                return None
        
        return BankerChequesDraftsRecord(
            source_table=table_name,
            timestamp_column_value=safe_str(raw_data[5]),  # transactionDate for tracking
            reporting_date=safe_str(raw_data[0]),
            customer_identification_number=safe_str(raw_data[1]),
            customer_name=safe_str(raw_data[2]),
            beneficiary_name=safe_str(raw_data[3]),
            check_number=safe_str(raw_data[4]),
            transaction_date=safe_str(raw_data[5]),
            value_date=safe_str(raw_data[6]),
            maturity_date=safe_str(raw_data[7]),
            currency=safe_str(raw_data[8]),
            org_amount=safe_float(raw_data[9]),
            usd_amount=safe_float(raw_data[10]),
            tzs_amount=safe_float(raw_data[11]),
            original_timestamp=datetime.now().isoformat()
        )
    
    def insert_to_postgres(self, record: BankerChequesDraftsRecord, pg_cursor) -> None:
        """Insert banker cheques and drafts record to PostgreSQL"""
        query = self.get_insert_query()
        
        pg_cursor.execute(query, (
            record.reporting_date,
            record.customer_identification_number,
            record.customer_name,
            record.beneficiary_name,
            record.check_number,
            record.transaction_date,
            record.value_date,
            record.maturity_date,
            record.currency,
            record.org_amount,
            record.usd_amount,
            record.tzs_amount
        ))
    
    def get_insert_query(self) -> str:
        """Get insert query for banker cheques and drafts (no unique constraint, keep all records)"""
        return """
        INSERT INTO "bankerChequesDrafts" (
            "reportingDate", "customerIdentificationNumber", "customerName", "beneficiaryName",
            "checkNumber", "transactionDate", "valueDate", "maturityDate", "currency",
            "orgAmount", "usdAmount", "tzsAmount"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
    
    def get_upsert_query(self) -> str:
        """Get upsert query for banker cheques and drafts (no unique constraint, just insert)"""
        # Since we want to keep all transaction records, just use insert
        return self.get_insert_query()
    
    def validate_record(self, record: BankerChequesDraftsRecord) -> bool:
        """Validate banker cheques and drafts record"""
        if not super().validate_record(record):
            return False
        
        # Banker cheques and drafts-specific validations
        if not record.customer_identification_number:
            return False
        if not record.transaction_date:
            return False
        if not record.value_date:
            return False
        if not record.currency:
            return False
        if record.org_amount is None or record.org_amount <= 0:
            return False
            
        return True
    
    def transform_data(self, raw_data: Tuple) -> dict:
        """Transform banker cheques and drafts data"""
        # Add any banker cheques and drafts-specific transformations here
        return {
            'customer_id_normalized': str(raw_data[1]).strip() if raw_data[1] else '',
            'currency_normalized': str(raw_data[8]).strip().upper() if raw_data[8] else 'TZS',
            'customer_name_normalized': str(raw_data[2]).strip().title() if raw_data[2] else None
        }