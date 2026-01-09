"""
Balances with MNOs processor
"""

from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime
from .base import BaseProcessor, BaseRecord

@dataclass
class MnosRecord(BaseRecord):
    """Balances with MNOs record structure"""
    reporting_date: str
    float_balance_date: str
    mno_code: str
    till_number: str
    currency: str
    allowance_probable_loss: float
    bot_provision: float
    org_float_amount: float
    usd_float_amount: Optional[float]
    tzs_float_amount: float
    retry_count: int = 0

class MnosProcessor(BaseProcessor):
    """Processor for balances with MNOs data"""
    
    def process_record(self, raw_data: Tuple, table_name: str) -> MnosRecord:
        """Convert raw DB2 data to MnosRecord"""
        return MnosRecord(
            source_table=table_name,
            timestamp_column_value=str(raw_data[0]),  # reportingDate
            reporting_date=str(raw_data[0]),
            float_balance_date=str(raw_data[1]),
            mno_code=str(raw_data[2]),
            till_number=str(raw_data[3]),
            currency=str(raw_data[4]),
            allowance_probable_loss=float(raw_data[5]),
            bot_provision=float(raw_data[6]),
            org_float_amount=float(raw_data[7]),
            usd_float_amount=float(raw_data[8]) if raw_data[8] else None,
            tzs_float_amount=float(raw_data[9]),
            original_timestamp=datetime.now().isoformat()
        )
    
    def insert_to_postgres(self, record: MnosRecord, pg_cursor) -> None:
        """Insert MNOs record to PostgreSQL with upsert on mnoCode"""
        query = self.get_upsert_query()
        
        pg_cursor.execute(query, (
            record.reporting_date,
            record.float_balance_date,
            record.mno_code,
            record.till_number,
            record.currency,
            record.allowance_probable_loss,
            record.bot_provision,
            record.org_float_amount,
            record.usd_float_amount,
            record.tzs_float_amount
        ))
    
    def get_upsert_query(self) -> str:
        """Get upsert query for balances with MNOs using mnoCode as unique constraint"""
        return """
        INSERT INTO "balanceWithMnos" (
            "reportingDate", "floatBalanceDate", "mnoCode", "tillNumber", "currency",
            "allowanceProbableLoss", "botProvision", "orgFloatAmount", "usdFloatAmount", "tzsFloatAmount"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT ("mnoCode") DO UPDATE SET
            "reportingDate" = EXCLUDED."reportingDate",
            "floatBalanceDate" = EXCLUDED."floatBalanceDate",
            "tillNumber" = EXCLUDED."tillNumber",
            "currency" = EXCLUDED."currency",
            "allowanceProbableLoss" = EXCLUDED."allowanceProbableLoss",
            "botProvision" = EXCLUDED."botProvision",
            "orgFloatAmount" = EXCLUDED."orgFloatAmount",
            "usdFloatAmount" = EXCLUDED."usdFloatAmount",
            "tzsFloatAmount" = EXCLUDED."tzsFloatAmount"
        """
    
    def validate_record(self, record: MnosRecord) -> bool:
        """Validate MNOs record"""
        if not super().validate_record(record):
            return False
        
        # MNOs-specific validations
        if not record.till_number:
            return False
        if record.org_float_amount is None:
            return False
        if not record.currency:
            return False
        if not record.mno_code:
            return False
            
        return True
    
    def transform_data(self, raw_data: Tuple) -> dict:
        """Transform MNOs data"""
        # Add any MNOs-specific transformations here
        return {
            'currency_normalized': str(raw_data[4]).strip().upper(),
            'amount_validated': max(0, float(raw_data[7])) if raw_data[7] else 0
        }