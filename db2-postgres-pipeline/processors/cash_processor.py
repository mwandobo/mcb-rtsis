"""
Cash information processor
"""

from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime
from .base import BaseProcessor, BaseRecord

@dataclass
class CashRecord(BaseRecord):
    """Cash information record structure"""
    trn_date: str
    reporting_date: str
    branch_code: str
    cash_category: str
    currency: str
    amount_local: float
    usd_amount: Optional[float]
    tzs_amount: float
    transaction_date: str
    maturity_date: str
    allowance_probable_loss: float = 0.0
    bot_provision: float = 0.0
    retry_count: int = 0

class CashProcessor(BaseProcessor):
    """Processor for cash information data"""
    
    def process_record(self, raw_data: Tuple, table_name: str) -> CashRecord:
        """Convert raw DB2 data to CashRecord"""
        return CashRecord(
            source_table=table_name,
            timestamp_column_value=str(raw_data[0]),
            trn_date=str(raw_data[0]),
            reporting_date=str(raw_data[1]),
            branch_code=str(raw_data[2]),
            cash_category=str(raw_data[3]),
            currency=str(raw_data[4]),
            amount_local=float(raw_data[5]),
            usd_amount=float(raw_data[6]) if raw_data[6] else None,
            tzs_amount=float(raw_data[7]),
            transaction_date=str(raw_data[8]),
            maturity_date=str(raw_data[9]),
            allowance_probable_loss=float(raw_data[10]),
            bot_provision=float(raw_data[11]),
            original_timestamp=datetime.now().isoformat()
        )
    
    def insert_to_postgres(self, record: CashRecord, pg_cursor) -> None:
        """Insert cash record to PostgreSQL"""
        query = self.get_upsert_query()
        
        pg_cursor.execute(query, (
            record.reporting_date,
            record.branch_code,
            record.cash_category,
            record.currency,
            record.amount_local,  # This maps to orgAmount in PostgreSQL
            record.usd_amount,
            record.tzs_amount,
            record.transaction_date,
            record.maturity_date,
            record.allowance_probable_loss,
            record.bot_provision
        ))
    
    def get_upsert_query(self) -> str:
        """Get insert query for cash information - incremental processing uses timestamp filtering"""
        return """
        INSERT INTO cash_information (
            "reportingDate", "branchCode", "cashCategory", currency,
            "orgAmount", "usdAmount", "tzsAmount", "transactionDate", "maturityDate",
            "allowanceProbableLoss", "botProvision"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
    
    def validate_record(self, record: CashRecord) -> bool:
        """Validate cash record"""
        if not super().validate_record(record):
            return False
        
        # Cash-specific validations
        if not record.branch_code:
            return False
        if record.amount_local is None:
            return False
        if not record.currency:
            return False
            
        return True
    
    def transform_data(self, raw_data: Tuple) -> dict:
        """Transform cash data"""
        # Add any cash-specific transformations here
        return {
            'currency_normalized': str(raw_data[4]).strip().upper(),
            'amount_validated': max(0, float(raw_data[5])) if raw_data[5] else 0
        }