"""
Cash information processor
"""

from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime
from .base import BaseProcessor, BaseRecord

@dataclass
class CashRecord(BaseRecord):
    """Cash information record structure - updated for 15-field format"""
    reporting_date: str
    branch_code: str
    cash_category: str
    cash_sub_category: Optional[str]
    cash_submission_time: str
    currency: str
    cash_denomination: Optional[str]
    quantity_of_coins_notes: Optional[int]
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
        """Convert raw DB2 data to CashRecord - updated for 15-field format"""
        # raw_data structure: reportingDate, branchCode, cashCategory, cashSubCategory, cashSubmissionTime, 
        # currency, cashDenomination, quantityOfCoinsNotes, orgAmount, usdAmount, tzsAmount, 
        # transactionDate, maturityDate, allowanceProbableLoss, botProvision
        return CashRecord(
            source_table=table_name,
            timestamp_column_value=str(raw_data[11]),  # transactionDate for tracking
            reporting_date=str(raw_data[0]),
            branch_code=str(raw_data[1]),
            cash_category=str(raw_data[2]),
            cash_sub_category=str(raw_data[3]) if raw_data[3] else None,
            cash_submission_time=str(raw_data[4]),
            currency=str(raw_data[5]).strip(),
            cash_denomination=str(raw_data[6]) if raw_data[6] else None,
            quantity_of_coins_notes=int(raw_data[7]) if raw_data[7] else None,
            amount_local=float(raw_data[8]) if raw_data[8] is not None else 0.0,
            usd_amount=float(raw_data[9]) if raw_data[9] is not None else None,
            tzs_amount=float(raw_data[10]) if raw_data[10] is not None else 0.0,
            transaction_date=str(raw_data[11]),
            maturity_date=str(raw_data[12]),
            allowance_probable_loss=float(raw_data[13]) if raw_data[13] is not None else 0.0,
            bot_provision=float(raw_data[14]) if raw_data[14] is not None else 0.0,
            original_timestamp=datetime.now().isoformat()
        )
    
    def insert_to_postgres(self, record: CashRecord, pg_cursor) -> None:
        """Insert cash record to PostgreSQL - updated for 15-field format"""
        query = self.get_upsert_query()
        
        pg_cursor.execute(query, (
            record.reporting_date,
            record.branch_code,
            record.cash_category,
            record.cash_sub_category,
            record.cash_submission_time,
            record.currency,
            record.cash_denomination,
            record.quantity_of_coins_notes,
            record.amount_local,  # This maps to orgAmount in PostgreSQL
            record.usd_amount,
            record.tzs_amount,
            record.transaction_date,
            record.maturity_date,
            record.allowance_probable_loss,
            record.bot_provision
        ))
    
    def get_upsert_query(self) -> str:
        """Get insert query for cash information - updated for 15-field format with camelCase"""
        return """
        INSERT INTO "cashInformation" (
            "reportingDate", "branchCode", "cashCategory", "cashSubCategory", "cashSubmissionTime",
            "currency", "cashDenomination", "quantityOfCoinsNotes", "orgAmount", "usdAmount", "tzsAmount", 
            "transactionDate", "maturityDate", "allowanceProbableLoss", "botProvision"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
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
        """Transform cash data - updated for 15-field format"""
        # Add any cash-specific transformations here
        return {
            'currency_normalized': str(raw_data[5]).strip().upper(),  # currency is now at index 5
            'amount_validated': max(0, float(raw_data[8])) if raw_data[8] else 0  # orgAmount is now at index 8
        }