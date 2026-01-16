"""
Cheque and other items for clearing processor - Based on cheque-and-other-items-for-clearing.sql
"""

from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime
from .base import BaseProcessor, BaseRecord

@dataclass
class ChequeClearingRecord(BaseRecord):
    """Cheque and other items for clearing record structure"""
    reporting_date: str
    cheque_number: str
    issuer_name: str
    issuer_banker_code: str
    payee_name: str
    payee_account_number: str
    cheque_date: str
    transaction_date: str
    settlement_date: str
    allowance_probable_loss: Optional[float]
    bot_provision: Optional[float]
    currency: str
    org_amount_opening: Optional[float]
    usd_amount_opening: Optional[float]
    tzs_amount_opening: Optional[float]
    org_amount_payment: Optional[float]
    usd_amount_payment: Optional[float]
    tzs_amount_payment: Optional[float]
    org_amount_balance: Optional[float]
    usd_amount_balance: Optional[float]
    tzs_amount_balance: Optional[float]
    retry_count: int = 0

class ChequeClearingProcessor(BaseProcessor):
    """Processor for cheque and other items for clearing data"""
    
    def process_record(self, raw_data: Tuple, table_name: str) -> ChequeClearingRecord:
        """Convert raw DB2 data to ChequeClearingRecord"""
        # Handle None values safely
        def safe_str(value):
            return str(value).strip() if value is not None else None
        
        def safe_float(value):
            try:
                return float(value) if value is not None else None
            except (ValueError, TypeError):
                return None
        
        return ChequeClearingRecord(
            source_table=table_name,
            timestamp_column_value=safe_str(raw_data[7]),  # transactionDate for tracking
            reporting_date=safe_str(raw_data[0]),
            cheque_number=safe_str(raw_data[1]),
            issuer_name=safe_str(raw_data[2]),
            issuer_banker_code=safe_str(raw_data[3]),
            payee_name=safe_str(raw_data[4]),
            payee_account_number=safe_str(raw_data[5]),
            cheque_date=safe_str(raw_data[6]),
            transaction_date=safe_str(raw_data[7]),
            settlement_date=safe_str(raw_data[8]),
            allowance_probable_loss=safe_float(raw_data[9]),
            bot_provision=safe_float(raw_data[10]),
            currency=safe_str(raw_data[11]),
            org_amount_opening=safe_float(raw_data[12]),
            usd_amount_opening=safe_float(raw_data[13]),
            tzs_amount_opening=safe_float(raw_data[14]),
            org_amount_payment=safe_float(raw_data[15]),
            usd_amount_payment=safe_float(raw_data[16]),
            tzs_amount_payment=safe_float(raw_data[17]),
            org_amount_balance=safe_float(raw_data[18]),
            usd_amount_balance=safe_float(raw_data[19]),
            tzs_amount_balance=safe_float(raw_data[20]),
            original_timestamp=datetime.now().isoformat()
        )
    
    def insert_to_postgres(self, record: ChequeClearingRecord, pg_cursor) -> None:
        """Insert cheque clearing record to PostgreSQL with upsert on chequeNumber"""
        query = self.get_upsert_query()
        
        pg_cursor.execute(query, (
            record.reporting_date,
            record.cheque_number,
            record.issuer_name,
            record.issuer_banker_code,
            record.payee_name,
            record.payee_account_number,
            record.cheque_date,
            record.transaction_date,
            record.settlement_date,
            record.allowance_probable_loss,
            record.bot_provision,
            record.currency,
            record.org_amount_opening,
            record.usd_amount_opening,
            record.tzs_amount_opening,
            record.org_amount_payment,
            record.usd_amount_payment,
            record.tzs_amount_payment,
            record.org_amount_balance,
            record.usd_amount_balance,
            record.tzs_amount_balance
        ))
    
    def get_upsert_query(self) -> str:
        """Get insert query for cheque clearing"""
        return """
        INSERT INTO "chequeClearing" (
            "reportingDate", "chequeNumber", "issuerName", "issuerBankerCode", "payeeName",
            "payeeAccountNumber", "chequeDate", "transactionDate", "settlementDate",
            "allowanceProbableLoss", "botProvision", "currency", "orgAmountOpening",
            "usdAmountOpening", "tzsAmountOpening", "orgAmountPayment", "usdAmountPayment",
            "tzsAmountPayment", "orgAmountBalance", "usdAmountBalance", "tzsAmountBalance"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
    
    def validate_record(self, record: ChequeClearingRecord) -> bool:
        """Validate cheque clearing record"""
        if not super().validate_record(record):
            return False
        
        # Cheque clearing-specific validations
        if not record.cheque_number:
            return False
        if not record.issuer_name:
            return False
        if not record.payee_name:
            return False
        if not record.currency:
            return False
            
        return True
    
    def transform_data(self, raw_data: Tuple) -> dict:
        """Transform cheque clearing data"""
        # Add any cheque clearing-specific transformations here
        return {
            'cheque_number_normalized': str(raw_data[1]).strip().upper() if raw_data[1] else '',
            'currency_normalized': str(raw_data[11]).strip().upper() if raw_data[11] else 'TZS',
            'issuer_banker_code_normalized': str(raw_data[3]).strip().upper() if raw_data[3] else 'UNKNOWN'
        }