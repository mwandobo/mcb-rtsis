#!/usr/bin/env python3
"""
Investment Debt Securities processor with streaming architecture
"""

from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime, date
from decimal import Decimal
from .base import BaseProcessor, BaseRecord

@dataclass
class InvestmentDebtSecuritiesRecord(BaseRecord):
    """Investment Debt Securities record structure"""
    reportingDate: str
    securityNumber: Optional[str]
    securityType: Optional[str]
    securityIssuerName: Optional[str]
    ratingStatus: Optional[str]
    externalIssuerRatting: Optional[str]
    gradesUnratedBanks: Optional[str]
    securityIssuerCountry: Optional[str]
    sectorSnaClassification: Optional[str]
    currency: Optional[str]
    orgCostValueAmount: Optional[Decimal]
    tzsCostValueAmount: Optional[Decimal]
    usdCostValueAmount: Optional[Decimal]
    orgFaceValueAmount: Optional[Decimal]
    tzsgFaceValueAmount: Optional[Decimal]
    usdgFaceValueAmount: Optional[Decimal]
    orgFairValueAmount: Optional[Decimal]
    tzsgFairValueAmount: Optional[Decimal]
    usdgFairValueAmount: Optional[Decimal]
    interestRate: Optional[Decimal]
    purchaseDate: Optional[date]
    valueDate: Optional[date]
    maturityDate: Optional[date]
    tradingIntent: Optional[str]
    securityEncumbaranceStatus: Optional[str]
    pastDueDays: Optional[int]
    allowanceProbableLoss: Optional[Decimal]
    botProvision: Optional[Decimal]
    assetClassificationCategory: Optional[str]
    retry_count: int = 0

class InvestmentDebtSecuritiesProcessor(BaseProcessor):
    """Processor for investment debt securities with streaming architecture"""
    
    def process_record(self, raw_data: Tuple, table_name: str) -> InvestmentDebtSecuritiesRecord:
        """Convert raw DB2 data to InvestmentDebtSecuritiesRecord"""
        return InvestmentDebtSecuritiesRecord(
            source_table=table_name,
            timestamp_column_value=str(raw_data[0]) if raw_data[0] else '',  # reportingDate
            reportingDate=str(raw_data[0]) if raw_data[0] else '',
            securityNumber=str(raw_data[1]).strip() if raw_data[1] else None,
            securityType=str(raw_data[2]).strip() if raw_data[2] else None,
            securityIssuerName=str(raw_data[3]).strip() if raw_data[3] else None,
            ratingStatus=str(raw_data[4]).strip() if raw_data[4] else None,
            externalIssuerRatting=str(raw_data[5]).strip() if raw_data[5] else None,
            gradesUnratedBanks=str(raw_data[6]).strip() if raw_data[6] else None,
            securityIssuerCountry=str(raw_data[7]).strip() if raw_data[7] else None,
            sectorSnaClassification=str(raw_data[8]).strip() if raw_data[8] else None,
            currency=str(raw_data[9]).strip() if raw_data[9] else None,
            orgCostValueAmount=Decimal(str(raw_data[10])) if raw_data[10] is not None else None,
            tzsCostValueAmount=Decimal(str(raw_data[11])) if raw_data[11] is not None else None,
            usdCostValueAmount=Decimal(str(raw_data[12])) if raw_data[12] is not None else None,
            orgFaceValueAmount=Decimal(str(raw_data[13])) if raw_data[13] is not None else None,
            tzsgFaceValueAmount=Decimal(str(raw_data[14])) if raw_data[14] is not None else None,
            usdgFaceValueAmount=Decimal(str(raw_data[15])) if raw_data[15] is not None else None,
            orgFairValueAmount=Decimal(str(raw_data[16])) if raw_data[16] is not None else None,
            tzsgFairValueAmount=Decimal(str(raw_data[17])) if raw_data[17] is not None else None,
            usdgFairValueAmount=Decimal(str(raw_data[18])) if raw_data[18] is not None else None,
            interestRate=Decimal(str(raw_data[19])) if raw_data[19] is not None else None,
            purchaseDate=raw_data[20] if isinstance(raw_data[20], date) else None,
            valueDate=raw_data[21] if isinstance(raw_data[21], date) else None,
            maturityDate=raw_data[22] if isinstance(raw_data[22], date) else None,
            tradingIntent=str(raw_data[23]).strip() if raw_data[23] else None,
            securityEncumbaranceStatus=str(raw_data[24]).strip() if raw_data[24] else None,
            pastDueDays=int(raw_data[25]) if raw_data[25] is not None else None,
            allowanceProbableLoss=Decimal(str(raw_data[26])) if raw_data[26] is not None else None,
            botProvision=Decimal(str(raw_data[27])) if raw_data[27] is not None else None,
            assetClassificationCategory=str(raw_data[28]).strip() if raw_data[28] else None,
            original_timestamp=datetime.now().isoformat()
        )
    
    def create_record_from_dict(self, record_data: dict) -> InvestmentDebtSecuritiesRecord:
        """Create InvestmentDebtSecuritiesRecord from dictionary (for RabbitMQ consumption)"""
        return InvestmentDebtSecuritiesRecord(
            source_table='investmentDebtSecurities',
            timestamp_column_value=record_data.get('reportingDate', ''),
            reportingDate=record_data.get('reportingDate', ''),
            securityNumber=record_data.get('securityNumber'),
            securityType=record_data.get('securityType'),
            securityIssuerName=record_data.get('securityIssuerName'),
            ratingStatus=record_data.get('ratingStatus'),
            externalIssuerRatting=record_data.get('externalIssuerRatting'),
            gradesUnratedBanks=record_data.get('gradesUnratedBanks'),
            securityIssuerCountry=record_data.get('securityIssuerCountry'),
            sectorSnaClassification=record_data.get('sectorSnaClassification'),
            currency=record_data.get('currency'),
            orgCostValueAmount=Decimal(str(record_data.get('orgCostValueAmount'))) if record_data.get('orgCostValueAmount') is not None else None,
            tzsCostValueAmount=Decimal(str(record_data.get('tzsCostValueAmount'))) if record_data.get('tzsCostValueAmount') is not None else None,
            usdCostValueAmount=Decimal(str(record_data.get('usdCostValueAmount'))) if record_data.get('usdCostValueAmount') is not None else None,
            orgFaceValueAmount=Decimal(str(record_data.get('orgFaceValueAmount'))) if record_data.get('orgFaceValueAmount') is not None else None,
            tzsgFaceValueAmount=Decimal(str(record_data.get('tzsgFaceValueAmount'))) if record_data.get('tzsgFaceValueAmount') is not None else None,
            usdgFaceValueAmount=Decimal(str(record_data.get('usdgFaceValueAmount'))) if record_data.get('usdgFaceValueAmount') is not None else None,
            orgFairValueAmount=Decimal(str(record_data.get('orgFairValueAmount'))) if record_data.get('orgFairValueAmount') is not None else None,
            tzsgFairValueAmount=Decimal(str(record_data.get('tzsgFairValueAmount'))) if record_data.get('tzsgFairValueAmount') is not None else None,
            usdgFairValueAmount=Decimal(str(record_data.get('usdgFairValueAmount'))) if record_data.get('usdgFairValueAmount') is not None else None,
            interestRate=Decimal(str(record_data.get('interestRate'))) if record_data.get('interestRate') is not None else None,
            purchaseDate=datetime.fromisoformat(record_data.get('purchaseDate')).date() if record_data.get('purchaseDate') else None,
            valueDate=datetime.fromisoformat(record_data.get('valueDate')).date() if record_data.get('valueDate') else None,
            maturityDate=datetime.fromisoformat(record_data.get('maturityDate')).date() if record_data.get('maturityDate') else None,
            tradingIntent=record_data.get('tradingIntent'),
            securityEncumbaranceStatus=record_data.get('securityEncumbaranceStatus'),
            pastDueDays=int(record_data.get('pastDueDays')) if record_data.get('pastDueDays') is not None else None,
            allowanceProbableLoss=Decimal(str(record_data.get('allowanceProbableLoss'))) if record_data.get('allowanceProbableLoss') is not None else None,
            botProvision=Decimal(str(record_data.get('botProvision'))) if record_data.get('botProvision') is not None else None,
            assetClassificationCategory=record_data.get('assetClassificationCategory'),
            original_timestamp=datetime.now().isoformat()
        )
    
    def insert_to_postgres(self, record: InvestmentDebtSecuritiesRecord, pg_cursor) -> None:
        """Insert Investment Debt Securities record to PostgreSQL"""
        query = """
        INSERT INTO "investmentDebtSecurities" (
            "reportingDate", "securityNumber", "securityType", "securityIssuerName", "ratingStatus",
            "externalIssuerRatting", "gradesUnratedBanks", "securityIssuerCountry", "sectorSnaClassification",
            "currency", "orgCostValueAmount", "tzsCostValueAmount", "usdCostValueAmount",
            "orgFaceValueAmount", "tzsgFaceValueAmount", "usdgFaceValueAmount", "orgFairValueAmount",
            "tzsgFairValueAmount", "usdgFairValueAmount", "interestRate", "purchaseDate", "valueDate",
            "maturityDate", "tradingIntent", "securityEncumbaranceStatus", "pastDueDays",
            "allowanceProbableLoss", "botProvision", "assetClassificationCategory"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        pg_cursor.execute(query, (
            record.reportingDate, record.securityNumber, record.securityType, record.securityIssuerName,
            record.ratingStatus, record.externalIssuerRatting, record.gradesUnratedBanks,
            record.securityIssuerCountry, record.sectorSnaClassification, record.currency,
            record.orgCostValueAmount, record.tzsCostValueAmount, record.usdCostValueAmount,
            record.orgFaceValueAmount, record.tzsgFaceValueAmount, record.usdgFaceValueAmount,
            record.orgFairValueAmount, record.tzsgFairValueAmount, record.usdgFairValueAmount,
            record.interestRate, record.purchaseDate, record.valueDate, record.maturityDate,
            record.tradingIntent, record.securityEncumbaranceStatus, record.pastDueDays,
            record.allowanceProbableLoss, record.botProvision, record.assetClassificationCategory
        ))
    
    def get_upsert_query(self) -> str:
        """Get upsert query for investment debt securities"""
        return self.insert_to_postgres.__doc__  # Same as insert for now
    
    def validate_record(self, record: InvestmentDebtSecuritiesRecord) -> bool:
        """Validate Investment Debt Securities record"""
        if not super().validate_record(record):
            return False
        
        # Basic validations
        if not record.securityNumber:
            return False
            
        return True