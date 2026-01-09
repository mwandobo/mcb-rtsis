"""
BOT balances processor
"""

from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime
from .base import BaseProcessor, BaseRecord

@dataclass
class BotBalancesRecord(BaseRecord):
    """BOT balances record structure"""
    reporting_date: str
    account_number: str
    account_name: str
    account_type: str
    sub_account_type: Optional[str]
    currency: str
    org_amount: float
    usd_amount: Optional[float]
    tzs_amount: float
    transaction_date: str
    maturity_date: str
    allowance_probable_loss: float = 0.0
    bot_provision: float = 0.0
    retry_count: int = 0

class BotBalancesProcessor(BaseProcessor):
    """Processor for BOT balances data"""
    
    def process_record(self, raw_data: Tuple, table_name: str) -> BotBalancesRecord:
        """Convert raw DB2 data to BotBalancesRecord"""
        return BotBalancesRecord(
            source_table=table_name,
            timestamp_column_value=str(raw_data[0]),  # reportingDate
            reporting_date=str(raw_data[0]),
            account_number=str(raw_data[1]),
            account_name=str(raw_data[2]),
            account_type=str(raw_data[3]),
            sub_account_type=str(raw_data[4]) if raw_data[4] else None,
            currency=str(raw_data[5]),
            org_amount=float(raw_data[6]),
            usd_amount=float(raw_data[7]) if raw_data[7] else None,
            tzs_amount=float(raw_data[8]),
            transaction_date=str(raw_data[9]),
            maturity_date=str(raw_data[10]),
            allowance_probable_loss=float(raw_data[11]),
            bot_provision=float(raw_data[12]),
            original_timestamp=datetime.now().isoformat()
        )
    
    def insert_to_postgres(self, record: BotBalancesRecord, pg_cursor) -> None:
        """Insert BOT balances record to PostgreSQL"""
        query = self.get_upsert_query()
        
        pg_cursor.execute(query, (
            record.reporting_date,
            record.account_number,
            record.account_name,
            record.account_type,
            record.sub_account_type,
            record.currency,
            record.org_amount,
            record.usd_amount,
            record.tzs_amount,
            record.transaction_date,
            record.maturity_date,
            record.allowance_probable_loss,
            record.bot_provision
        ))
    
    def get_upsert_query(self) -> str:
        """Get insert query for BOT transactions (no upsert needed for transaction data)"""
        return """
        INSERT INTO "balancesBot" (
            "reportingDate", "accountNumber", "accountName", "accountType", "subAccountType",
            "currency", "orgAmount", "usdAmount", "tzsAmount", "transactionDate", "maturityDate",
            "allowanceProbableLoss", "botProvision"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
    
    def validate_record(self, record: BotBalancesRecord) -> bool:
        """Validate BOT balances record"""
        if not super().validate_record(record):
            return False
        
        # BOT balances-specific validations
        if not record.account_number:
            return False
        if record.org_amount is None:
            return False
        if not record.currency:
            return False
        if not record.account_name:
            return False
            
        return True
    
    def transform_data(self, raw_data: Tuple) -> dict:
        """Transform BOT balances data"""
        # Add any BOT balances-specific transformations here
        return {
            'currency_normalized': str(raw_data[5]).strip().upper(),
            'amount_validated': max(0, float(raw_data[6])) if raw_data[6] else 0
        }