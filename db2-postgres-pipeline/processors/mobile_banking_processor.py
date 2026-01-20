"""
Mobile Banking processor with camelCase naming
"""

from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime, date
from decimal import Decimal
from .base import BaseProcessor, BaseRecord

@dataclass
class MobileBankingRecord(BaseRecord):
    """Mobile Banking record structure with camelCase fields"""
    reportingDate: str
    transactionDate: date
    accountNumber: Optional[str]
    customerIdentificationNumber: Optional[str]
    mobileTransactionType: str
    serviceCategory: str
    subServiceCategory: Optional[str]
    serviceStatus: str
    transactionRef: str
    benBankOrWalletCode: str
    benAccountOrMobileNumber: Optional[str]
    currency: str
    orgAmount: Decimal
    tzsAmount: Optional[Decimal]
    valueAddedTaxAmount: Optional[Decimal]
    exciseDutyAmount: Optional[Decimal]
    electronicLevyAmount: Optional[Decimal]
    retry_count: int = 0

class MobileBankingProcessor(BaseProcessor):
    """Processor for mobile banking data with camelCase naming"""
    
    def process_record(self, raw_data: Tuple, table_name: str) -> MobileBankingRecord:
        """Convert raw DB2 data to MobileBankingRecord"""
        return MobileBankingRecord(
            source_table=table_name,
            timestamp_column_value=str(raw_data[0]) if raw_data[0] else '',  # reportingDate
            reportingDate=str(raw_data[0]) if raw_data[0] else '',
            transactionDate=raw_data[1] if isinstance(raw_data[1], date) else date.today(),
            accountNumber=str(raw_data[2]).strip() if raw_data[2] else None,
            customerIdentificationNumber=str(raw_data[3]).strip() if raw_data[3] else None,
            mobileTransactionType=str(raw_data[4]).strip() if raw_data[4] else '',
            serviceCategory=str(raw_data[5]).strip() if raw_data[5] else '',
            subServiceCategory=str(raw_data[6]).strip() if raw_data[6] else None,
            serviceStatus=str(raw_data[7]).strip() if raw_data[7] else '',
            transactionRef=str(raw_data[8]).strip() if raw_data[8] else '',
            benBankOrWalletCode=str(raw_data[9]).strip() if raw_data[9] else '',
            benAccountOrMobileNumber=str(raw_data[10]).strip() if raw_data[10] else None,
            currency=str(raw_data[11]).strip() if raw_data[11] else '',
            orgAmount=Decimal(str(raw_data[12])) if raw_data[12] is not None else Decimal('0'),
            tzsAmount=Decimal(str(raw_data[13])) if raw_data[13] is not None else None,
            valueAddedTaxAmount=Decimal(str(raw_data[14])) if raw_data[14] is not None else None,
            exciseDutyAmount=Decimal(str(raw_data[15])) if raw_data[15] is not None else None,
            electronicLevyAmount=Decimal(str(raw_data[16])) if raw_data[16] is not None else None,
            # Note: raw_data[17] is trn_snum which we don't store in the record, just use for cursor
            original_timestamp=datetime.now().isoformat()
        )
    
    def create_record_from_dict(self, record_data: dict) -> MobileBankingRecord:
        """Create MobileBankingRecord from dictionary (for RabbitMQ consumption)"""
        return MobileBankingRecord(
            source_table='mobileBanking',
            timestamp_column_value=record_data.get('reportingDate', ''),
            reportingDate=record_data.get('reportingDate', ''),
            transactionDate=datetime.fromisoformat(record_data.get('transactionDate')).date() if record_data.get('transactionDate') else date.today(),
            accountNumber=record_data.get('accountNumber'),
            customerIdentificationNumber=record_data.get('customerIdentificationNumber'),
            mobileTransactionType=record_data.get('mobileTransactionType', ''),
            serviceCategory=record_data.get('serviceCategory', ''),
            subServiceCategory=record_data.get('subServiceCategory'),
            serviceStatus=record_data.get('serviceStatus', ''),
            transactionRef=record_data.get('transactionRef', ''),
            benBankOrWalletCode=record_data.get('benBankOrWalletCode', ''),
            benAccountOrMobileNumber=record_data.get('benAccountOrMobileNumber'),
            currency=record_data.get('currency', ''),
            orgAmount=Decimal(str(record_data.get('orgAmount', '0'))),
            tzsAmount=Decimal(str(record_data.get('tzsAmount'))) if record_data.get('tzsAmount') is not None else None,
            valueAddedTaxAmount=Decimal(str(record_data.get('valueAddedTaxAmount'))) if record_data.get('valueAddedTaxAmount') is not None else None,
            exciseDutyAmount=Decimal(str(record_data.get('exciseDutyAmount'))) if record_data.get('exciseDutyAmount') is not None else None,
            electronicLevyAmount=Decimal(str(record_data.get('electronicLevyAmount'))) if record_data.get('electronicLevyAmount') is not None else None,
            original_timestamp=datetime.now().isoformat()
        )
    
    def insert_to_postgres(self, record: MobileBankingRecord, pg_cursor) -> None:
        """Insert Mobile Banking record to PostgreSQL with camelCase table and fields"""
        query = """
        INSERT INTO "mobileBanking" (
            "reportingDate", "transactionDate", "accountNumber", "customerIdentificationNumber",
            "mobileTransactionType", "serviceCategory", "subServiceCategory", "serviceStatus",
            "transactionRef", "benBankOrWalletCode", "benAccountOrMobileNumber", "currency",
            "orgAmount", "tzsAmount", "valueAddedTaxAmount", "exciseDutyAmount", "electronicLevyAmount"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        pg_cursor.execute(query, (
            record.reportingDate,
            record.transactionDate,
            record.accountNumber,
            record.customerIdentificationNumber,
            record.mobileTransactionType,
            record.serviceCategory,
            record.subServiceCategory,
            record.serviceStatus,
            record.transactionRef,
            record.benBankOrWalletCode,
            record.benAccountOrMobileNumber,
            record.currency,
            record.orgAmount,
            record.tzsAmount,
            record.valueAddedTaxAmount,
            record.exciseDutyAmount,
            record.electronicLevyAmount
        ))
    
    def get_upsert_query(self) -> str:
        """Get upsert query for mobileBanking"""
        return """
        INSERT INTO "mobileBanking" (
            "reportingDate", "transactionDate", "accountNumber", "customerIdentificationNumber",
            "mobileTransactionType", "serviceCategory", "subServiceCategory", "serviceStatus",
            "transactionRef", "benBankOrWalletCode", "benAccountOrMobileNumber", "currency",
            "orgAmount", "tzsAmount", "valueAddedTaxAmount", "exciseDutyAmount", "electronicLevyAmount"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
    
    def validate_record(self, record: MobileBankingRecord) -> bool:
        """Validate Mobile Banking record"""
        if not super().validate_record(record):
            return False
        
        # Basic validations
        if not record.transactionRef:
            return False
        if not record.mobileTransactionType:
            return False
        if not record.currency:
            return False
            
        return True