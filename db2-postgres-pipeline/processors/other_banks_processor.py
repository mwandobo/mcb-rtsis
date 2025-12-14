"""
Balance with Other Banks processor
"""

from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime
from .base import BaseProcessor, BaseRecord

@dataclass
class OtherBanksRecord(BaseRecord):
    """Balance with Other Banks record structure"""
    reporting_date: str
    account_number: str
    account_name: str
    bank_code: str
    country: str
    relationship_type: str
    account_type: str
    sub_account_type: str
    currency: str
    org_amount: float
    usd_amount: Optional[float]
    tzs_amount: float
    transaction_date: str
    past_due_days: int
    allowance_probable_loss: float
    bot_provision: float
    assets_classification_category: str
    contract_date: str
    maturity_date: str
    external_rating_correspondent_bank: str
    grades_unrated_banks: str
    retry_count: int = 0

class OtherBanksProcessor(BaseProcessor):
    """Processor for balance with other banks data"""
    
    def process_record(self, raw_data: Tuple, table_name: str) -> OtherBanksRecord:
        """Convert raw DB2 data to OtherBanksRecord"""
        return OtherBanksRecord(
            source_table=table_name,
            timestamp_column_value=str(raw_data[0]),  # reportingDate
            reporting_date=str(raw_data[0]),
            account_number=str(raw_data[1]),
            account_name=str(raw_data[2]),
            bank_code=str(raw_data[3]),
            country=str(raw_data[4]),
            relationship_type=str(raw_data[5]),
            account_type=str(raw_data[6]),
            sub_account_type=str(raw_data[7]),
            currency=str(raw_data[8]),
            org_amount=float(raw_data[9]),
            usd_amount=float(raw_data[10]) if raw_data[10] else None,
            tzs_amount=float(raw_data[11]),
            transaction_date=str(raw_data[12]),
            past_due_days=int(raw_data[13]) if raw_data[13] else 0,
            allowance_probable_loss=float(raw_data[14]),
            bot_provision=float(raw_data[15]),
            assets_classification_category=str(raw_data[16]),
            contract_date=str(raw_data[17]),
            maturity_date=str(raw_data[18]),
            external_rating_correspondent_bank=str(raw_data[19]),
            grades_unrated_banks=str(raw_data[20]),
            original_timestamp=datetime.now().isoformat()
        )
    
    def insert_to_postgres(self, record: OtherBanksRecord, pg_cursor) -> None:
        """Insert Other Banks record to PostgreSQL"""
        query = self.get_upsert_query()
        
        pg_cursor.execute(query, (
            record.reporting_date,
            record.account_number,
            record.account_name,
            record.bank_code,
            record.country,
            record.relationship_type,
            record.account_type,
            record.sub_account_type,
            record.currency,
            record.org_amount,
            record.usd_amount,
            record.tzs_amount,
            record.transaction_date,
            record.past_due_days,
            record.allowance_probable_loss,
            record.bot_provision,
            record.assets_classification_category,
            record.contract_date,
            record.maturity_date,
            record.external_rating_correspondent_bank,
            record.grades_unrated_banks
        ))
    
    def get_upsert_query(self) -> str:
        """Get insert query for balance with other banks"""
        return """
        INSERT INTO balance_with_other_bank (
            "reportingDate", "accountNumber", "accountName", "bankCode", country,
            "relationshipType", "accountType", "subAccountType", currency,
            "orgAmount", "usdAmount", "tzsAmount", "transactionDate", "pastDueDays",
            "allowanceProbableLoss", "botProvision", "assetsClassificationCategory",
            "contractDate", "maturityDate", "externalRatingCorrespondentBank", "gradesUnratedBanks"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )

        """
    
    def validate_record(self, record: OtherBanksRecord) -> bool:
        """Validate Other Banks record"""
        if not super().validate_record(record):
            return False
        
        # Other Banks-specific validations
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
        """Transform Other Banks data"""
        # Add any Other Banks-specific transformations here
        return {
            'currency_normalized': str(raw_data[8]).strip().upper(),
            'amount_validated': max(0, float(raw_data[9])) if raw_data[9] else 0
        }