"""
Simple Balance with Other Banks processor
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
    sub_account_type: Optional[str]
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
    grades_unrated_banks: Optional[str]
    retry_count: int = 0

class OtherBanksProcessor(BaseProcessor):
    """Simple processor for balance with other banks data"""
    
    def process_record(self, raw_data: Tuple, table_name: str) -> OtherBanksRecord:
        """Convert raw DB2 data to OtherBanksRecord"""
        return OtherBanksRecord(
            source_table=table_name,
            timestamp_column_value=str(raw_data[0]),
            reporting_date=str(raw_data[0]) if raw_data[0] else '',
            account_number=str(raw_data[1]) if raw_data[1] else '',
            account_name=str(raw_data[2]) if raw_data[2] else '',
            bank_code=str(raw_data[3]) if raw_data[3] else '',
            country=str(raw_data[4]) if raw_data[4] else '',
            relationship_type=str(raw_data[5]) if raw_data[5] else '',
            account_type=str(raw_data[6]) if raw_data[6] else '',
            sub_account_type=str(raw_data[7]) if raw_data[7] else None,
            currency=str(raw_data[8]) if raw_data[8] else '',
            org_amount=float(raw_data[9]) if raw_data[9] is not None else 0.0,
            usd_amount=float(raw_data[10]) if raw_data[10] is not None else None,
            tzs_amount=float(raw_data[11]) if raw_data[11] is not None else 0.0,
            transaction_date=str(raw_data[12]) if raw_data[12] else '',
            past_due_days=int(raw_data[13]) if raw_data[13] is not None else 0,
            allowance_probable_loss=float(raw_data[14]) if raw_data[14] is not None else 0.0,
            bot_provision=float(raw_data[15]) if raw_data[15] is not None else 0.0,
            assets_classification_category=str(raw_data[16]) if raw_data[16] else '',
            contract_date=str(raw_data[17]) if raw_data[17] else '',
            maturity_date=str(raw_data[18]) if raw_data[18] else '',
            external_rating_correspondent_bank=str(raw_data[19]) if raw_data[19] else '',
            grades_unrated_banks=str(raw_data[20]) if raw_data[20] else None,
            original_timestamp=datetime.now().isoformat()
        )
    
    def create_record_from_dict(self, record_data: dict) -> OtherBanksRecord:
        """Create OtherBanksRecord from dictionary (for RabbitMQ consumption)"""
        return OtherBanksRecord(
            source_table='balance_with_other_banks',
            timestamp_column_value=record_data.get('reportingDate', ''),
            reporting_date=record_data.get('reportingDate', ''),
            account_number=record_data.get('accountNumber', ''),
            account_name=record_data.get('accountName', ''),
            bank_code=record_data.get('bankCode', ''),
            country=record_data.get('country', ''),
            relationship_type=record_data.get('relationshipType', ''),
            account_type=record_data.get('accountType', ''),
            sub_account_type=record_data.get('subAccountType'),
            currency=record_data.get('currency', ''),
            org_amount=float(record_data.get('orgAmount', 0)) if record_data.get('orgAmount') is not None else 0.0,
            usd_amount=float(record_data.get('usdAmount')) if record_data.get('usdAmount') is not None else None,
            tzs_amount=float(record_data.get('tzsAmount', 0)) if record_data.get('tzsAmount') is not None else 0.0,
            transaction_date=record_data.get('transactionDate', ''),
            past_due_days=int(record_data.get('pastDueDays', 0)) if record_data.get('pastDueDays') is not None else 0,
            allowance_probable_loss=float(record_data.get('allowanceProbableLoss', 0)) if record_data.get('allowanceProbableLoss') is not None else 0.0,
            bot_provision=float(record_data.get('botProvision', 0)) if record_data.get('botProvision') is not None else 0.0,
            assets_classification_category=record_data.get('assetsClassificationCategory', ''),
            contract_date=record_data.get('contractDate', ''),
            maturity_date=record_data.get('maturityDate', ''),
            external_rating_correspondent_bank=record_data.get('externalRatingCorrespondentBank', ''),
            grades_unrated_banks=record_data.get('gradesUnratedBanks'),
            original_timestamp=datetime.now().isoformat()
        )
    
    def insert_to_postgres(self, record: OtherBanksRecord, pg_cursor) -> None:
        """Insert Other Banks record to PostgreSQL"""
        query = """
        INSERT INTO "balanceWithOtherBank" (
            "reportingDate", "accountNumber", "accountName", "bankCode", "country",
            "relationshipType", "accountType", "subAccountType", "currency",
            "orgAmount", "usdAmount", "tzsAmount", "transactionDate", "pastDueDays",
            "allowanceProbableLoss", "botProvision", "assetsClassificationCategory",
            "contractDate", "maturityDate", "externalRatingCorrespondentBank", "gradesUnratedBanks"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
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
        """Get upsert query for balance with other banks"""
        return """
        INSERT INTO "balanceWithOtherBank" (
            "reportingDate", "accountNumber", "accountName", "bankCode", "country",
            "relationshipType", "accountType", "subAccountType", "currency",
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
        
        # Basic validations
        if not record.account_number:
            return False
        if not record.currency:
            return False
        if not record.account_name:
            return False
            
        return True