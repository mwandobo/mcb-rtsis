"""
Deposits processor with camelCase naming
"""

from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime, date
from decimal import Decimal
from .base import BaseProcessor, BaseRecord

@dataclass
class DepositsRecord(BaseRecord):
    """Deposits record structure with camelCase fields"""
    reportingDate: str
    clientIdentificationNumber: Optional[str]
    accountNumber: Optional[str]
    accountName: Optional[str]
    customerCategory: Optional[str]
    customerCountry: str
    branchCode: Optional[str]
    clientType: Optional[str]
    relationshipType: str
    district: Optional[str]
    region: str
    accountProductName: Optional[str]
    accountType: str
    accountSubType: Optional[str]
    depositCategory: str
    depositAccountStatus: str
    transactionUniqueRef: str
    timeStamp: Optional[str]
    serviceChannel: str
    currency: str
    transactionType: str
    orgTransactionAmount: Decimal
    usdTransactionAmount: Optional[Decimal]
    tzsTransactionAmount: Decimal
    transactionPurposes: Optional[str]
    sectorSnaClassification: Optional[str]
    lienNumber: Optional[str]
    orgAmountLien: Optional[Decimal]
    usdAmountLien: Optional[Decimal]
    tzsAmountLien: Optional[Decimal]
    contractDate: Optional[date]
    maturityDate: Optional[date]
    annualInterestRate: Optional[Decimal]
    interestRateType: Optional[str]
    orgInterestAmount: Decimal
    usdInterestAmount: Decimal
    tzsInterestAmount: Decimal
    retry_count: int = 0

class DepositsProcessor(BaseProcessor):
    """Processor for deposits data with camelCase naming"""
    
    def process_record(self, raw_data: Tuple, table_name: str) -> DepositsRecord:
        """Convert raw DB2 data to DepositsRecord"""
        return DepositsRecord(
            source_table=table_name,
            timestamp_column_value=str(raw_data[0]) if raw_data[0] else '',  # reportingDate
            reportingDate=str(raw_data[0]) if raw_data[0] else '',
            clientIdentificationNumber=str(raw_data[1]).strip() if raw_data[1] else None,
            accountNumber=str(raw_data[2]).strip() if raw_data[2] else None,
            accountName=str(raw_data[3]).strip() if raw_data[3] else None,
            customerCategory=str(raw_data[4]).strip() if raw_data[4] else None,
            customerCountry=str(raw_data[5]).strip() if raw_data[5] else 'TANZANIA, UNITED REPUBLIC OF',
            branchCode=str(raw_data[6]).strip() if raw_data[6] else None,
            clientType=str(raw_data[7]).strip() if raw_data[7] else None,
            relationshipType=str(raw_data[8]).strip() if raw_data[8] else 'Domestic banks unrelated',
            district=str(raw_data[9]).strip() if raw_data[9] else None,
            region=str(raw_data[10]).strip() if raw_data[10] else 'DAR ES SALAAM',
            accountProductName=str(raw_data[11]).strip() if raw_data[11] else None,
            accountType=str(raw_data[12]).strip() if raw_data[12] else 'Saving',
            accountSubType=str(raw_data[13]).strip() if raw_data[13] else None,
            depositCategory=str(raw_data[14]).strip() if raw_data[14] else 'Deposit from public',
            depositAccountStatus=str(raw_data[15]).strip() if raw_data[15] else '',
            transactionUniqueRef=str(raw_data[16]).strip() if raw_data[16] else '',
            timeStamp=str(raw_data[17]) if raw_data[17] else None,
            serviceChannel=str(raw_data[18]).strip() if raw_data[18] else 'Branch',
            currency=str(raw_data[19]).strip() if raw_data[19] else '',
            transactionType=str(raw_data[20]).strip() if raw_data[20] else 'Deposit',
            orgTransactionAmount=Decimal(str(raw_data[21])) if raw_data[21] is not None else Decimal('0'),
            usdTransactionAmount=Decimal(str(raw_data[22])) if raw_data[22] is not None else None,
            tzsTransactionAmount=Decimal(str(raw_data[23])) if raw_data[23] is not None else Decimal('0'),
            transactionPurposes=str(raw_data[24]).strip() if raw_data[24] else None,
            sectorSnaClassification=str(raw_data[25]).strip() if raw_data[25] else None,
            lienNumber=str(raw_data[26]).strip() if raw_data[26] else None,
            orgAmountLien=Decimal(str(raw_data[27])) if raw_data[27] is not None else None,
            usdAmountLien=Decimal(str(raw_data[28])) if raw_data[28] is not None else None,
            tzsAmountLien=Decimal(str(raw_data[29])) if raw_data[29] is not None else None,
            contractDate=raw_data[30] if isinstance(raw_data[30], date) else None,
            maturityDate=raw_data[31] if isinstance(raw_data[31], date) else None,
            annualInterestRate=Decimal(str(raw_data[32])) if raw_data[32] is not None else None,
            interestRateType=str(raw_data[33]).strip() if raw_data[33] else None,
            orgInterestAmount=Decimal(str(raw_data[34])) if raw_data[34] is not None else Decimal('0'),
            usdInterestAmount=Decimal(str(raw_data[35])) if raw_data[35] is not None else Decimal('0'),
            tzsInterestAmount=Decimal(str(raw_data[36])) if raw_data[36] is not None else Decimal('0'),
            # Note: raw_data[37] would be rn (row number) for pagination - we ignore it
            original_timestamp=datetime.now().isoformat()
        )
    
    def create_record_from_dict(self, record_data: dict) -> DepositsRecord:
        """Create DepositsRecord from dictionary (for RabbitMQ consumption)"""
        return DepositsRecord(
            source_table='deposits',
            timestamp_column_value=record_data.get('reportingDate', ''),
            reportingDate=record_data.get('reportingDate', ''),
            clientIdentificationNumber=record_data.get('clientIdentificationNumber'),
            accountNumber=record_data.get('accountNumber'),
            accountName=record_data.get('accountName'),
            customerCategory=record_data.get('customerCategory'),
            customerCountry=record_data.get('customerCountry', 'TANZANIA, UNITED REPUBLIC OF'),
            branchCode=record_data.get('branchCode'),
            clientType=record_data.get('clientType'),
            relationshipType=record_data.get('relationshipType', 'Domestic banks unrelated'),
            district=record_data.get('district'),
            region=record_data.get('region', 'DAR ES SALAAM'),
            accountProductName=record_data.get('accountProductName'),
            accountType=record_data.get('accountType', 'Saving'),
            accountSubType=record_data.get('accountSubType'),
            depositCategory=record_data.get('depositCategory', 'Deposit from public'),
            depositAccountStatus=record_data.get('depositAccountStatus', ''),
            transactionUniqueRef=record_data.get('transactionUniqueRef', ''),
            timeStamp=record_data.get('timeStamp'),
            serviceChannel=record_data.get('serviceChannel', 'Branch'),
            currency=record_data.get('currency', ''),
            transactionType=record_data.get('transactionType', 'Deposit'),
            orgTransactionAmount=Decimal(str(record_data.get('orgTransactionAmount', '0'))),
            usdTransactionAmount=Decimal(str(record_data.get('usdTransactionAmount'))) if record_data.get('usdTransactionAmount') is not None else None,
            tzsTransactionAmount=Decimal(str(record_data.get('tzsTransactionAmount', '0'))),
            transactionPurposes=record_data.get('transactionPurposes'),
            sectorSnaClassification=record_data.get('sectorSnaClassification'),
            lienNumber=record_data.get('lienNumber'),
            orgAmountLien=Decimal(str(record_data.get('orgAmountLien'))) if record_data.get('orgAmountLien') is not None else None,
            usdAmountLien=Decimal(str(record_data.get('usdAmountLien'))) if record_data.get('usdAmountLien') is not None else None,
            tzsAmountLien=Decimal(str(record_data.get('tzsAmountLien'))) if record_data.get('tzsAmountLien') is not None else None,
            contractDate=datetime.fromisoformat(record_data.get('contractDate')).date() if record_data.get('contractDate') else None,
            maturityDate=datetime.fromisoformat(record_data.get('maturityDate')).date() if record_data.get('maturityDate') else None,
            annualInterestRate=Decimal(str(record_data.get('annualInterestRate'))) if record_data.get('annualInterestRate') is not None else None,
            interestRateType=record_data.get('interestRateType'),
            orgInterestAmount=Decimal(str(record_data.get('orgInterestAmount', '0'))),
            usdInterestAmount=Decimal(str(record_data.get('usdInterestAmount', '0'))),
            tzsInterestAmount=Decimal(str(record_data.get('tzsInterestAmount', '0'))),
            original_timestamp=datetime.now().isoformat()
        )
    
    def insert_to_postgres(self, record: DepositsRecord, pg_cursor) -> None:
        """Insert Deposits record to PostgreSQL with camelCase table and fields"""
        query = """
        INSERT INTO "deposits" (
            "reportingDate", "clientIdentificationNumber", "accountNumber", "accountName",
            "customerCategory", "customerCountry", "branchCode", "clientType", "relationshipType",
            "district", "region", "accountProductName", "accountType", "accountSubType",
            "depositCategory", "depositAccountStatus", "transactionUniqueRef", "timeStamp",
            "serviceChannel", "currency", "transactionType", "orgTransactionAmount",
            "usdTransactionAmount", "tzsTransactionAmount", "transactionPurposes",
            "sectorSnaClassification", "lienNumber", "orgAmountLien", "usdAmountLien",
            "tzsAmountLien", "contractDate", "maturityDate", "annualInterestRate",
            "interestRateType", "orgInterestAmount", "usdInterestAmount", "tzsInterestAmount"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        pg_cursor.execute(query, (
            record.reportingDate,
            record.clientIdentificationNumber,
            record.accountNumber,
            record.accountName,
            record.customerCategory,
            record.customerCountry,
            record.branchCode,
            record.clientType,
            record.relationshipType,
            record.district,
            record.region,
            record.accountProductName,
            record.accountType,
            record.accountSubType,
            record.depositCategory,
            record.depositAccountStatus,
            record.transactionUniqueRef,
            record.timeStamp,
            record.serviceChannel,
            record.currency,
            record.transactionType,
            record.orgTransactionAmount,
            record.usdTransactionAmount,
            record.tzsTransactionAmount,
            record.transactionPurposes,
            record.sectorSnaClassification,
            record.lienNumber,
            record.orgAmountLien,
            record.usdAmountLien,
            record.tzsAmountLien,
            record.contractDate,
            record.maturityDate,
            record.annualInterestRate,
            record.interestRateType,
            record.orgInterestAmount,
            record.usdInterestAmount,
            record.tzsInterestAmount
        ))
    
    def get_upsert_query(self) -> str:
        """Get upsert query for deposits"""
        return """
        INSERT INTO "deposits" (
            "reportingDate", "clientIdentificationNumber", "accountNumber", "accountName",
            "customerCategory", "customerCountry", "branchCode", "clientType", "relationshipType",
            "district", "region", "accountProductName", "accountType", "accountSubType",
            "depositCategory", "depositAccountStatus", "transactionUniqueRef", "timeStamp",
            "serviceChannel", "currency", "transactionType", "orgTransactionAmount",
            "usdTransactionAmount", "tzsTransactionAmount", "transactionPurposes",
            "sectorSnaClassification", "lienNumber", "orgAmountLien", "usdAmountLien",
            "tzsAmountLien", "contractDate", "maturityDate", "annualInterestRate",
            "interestRateType", "orgInterestAmount", "usdInterestAmount", "tzsInterestAmount"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
    
    def validate_record(self, record: DepositsRecord) -> bool:
        """Validate Deposits record"""
        if not super().validate_record(record):
            return False
        
        # Basic validations
        if not record.transactionUniqueRef:
            return False
        if not record.currency:
            return False
        if not record.transactionType:
            return False
            
        return True