#!/usr/bin/env python3
"""
Interbank Loan Payable processor with streaming architecture
"""

from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime, date
from decimal import Decimal
from .base import BaseProcessor, BaseRecord

@dataclass
class InterbankLoanPayableRecord(BaseRecord):
    """Interbank Loan Payable record structure"""
    reportingDate: str
    lenderName: Optional[str]
    accountNumber: Optional[str]
    lenderCountry: Optional[str]
    borrowingType: Optional[str]
    transactionDate: Optional[date]
    disbursementDate: Optional[date]
    maturityDate: Optional[date]
    currency: Optional[str]
    orgAmountOpening: Optional[Decimal]
    usdAmountOpening: Optional[Decimal]
    tzsAmountOpening: Optional[Decimal]
    orgAmountRepayment: Optional[Decimal]
    usdAmountRepayment: Optional[Decimal]
    tzsAmountRepayment: Optional[Decimal]
    orgAmountClosing: Optional[Decimal]
    usdAmountClosing: Optional[Decimal]
    tzsAmountClosing: Optional[Decimal]
    tenureDays: Optional[int]
    annualInterestRate: Optional[Decimal]
    interestRateType: Optional[str]
    retry_count: int = 0

class InterbankLoanPayableProcessor(BaseProcessor):
    """Processor for interbank loan payable with streaming architecture"""
    
    def process_record(self, raw_data: Tuple, table_name: str) -> InterbankLoanPayableRecord:
        """Convert raw DB2 data to InterbankLoanPayableRecord"""
        return InterbankLoanPayableRecord(
            source_table=table_name,
            timestamp_column_value=str(raw_data[0]) if raw_data[0] else '',  # reportingDate
            reportingDate=str(raw_data[0]) if raw_data[0] else '',
            lenderName=str(raw_data[1]).strip() if raw_data[1] else None,
            accountNumber=str(raw_data[2]).strip() if raw_data[2] else None,
            lenderCountry=str(raw_data[3]).strip() if raw_data[3] else None,
            borrowingType=str(raw_data[4]).strip() if raw_data[4] else None,
            transactionDate=raw_data[5] if isinstance(raw_data[5], date) else None,
            disbursementDate=raw_data[6] if isinstance(raw_data[6], date) else None,
            maturityDate=raw_data[7] if isinstance(raw_data[7], date) else None,
            currency=str(raw_data[8]).strip() if raw_data[8] else None,
            orgAmountOpening=Decimal(str(raw_data[9])) if raw_data[9] is not None else None,
            usdAmountOpening=Decimal(str(raw_data[10])) if raw_data[10] is not None else None,
            tzsAmountOpening=Decimal(str(raw_data[11])) if raw_data[11] is not None else None,
            orgAmountRepayment=Decimal(str(raw_data[12])) if raw_data[12] is not None else None,
            usdAmountRepayment=Decimal(str(raw_data[13])) if raw_data[13] is not None else None,
            tzsAmountRepayment=Decimal(str(raw_data[14])) if raw_data[14] is not None else None,
            orgAmountClosing=Decimal(str(raw_data[15])) if raw_data[15] is not None else None,
            usdAmountClosing=Decimal(str(raw_data[16])) if raw_data[16] is not None else None,
            tzsAmountClosing=Decimal(str(raw_data[17])) if raw_data[17] is not None else None,
            tenureDays=int(raw_data[18]) if raw_data[18] is not None else None,
            annualInterestRate=Decimal(str(raw_data[19])) if raw_data[19] is not None else None,
            interestRateType=str(raw_data[20]).strip() if raw_data[20] else None,
            original_timestamp=datetime.now().isoformat()
        )
    
    def create_record_from_dict(self, record_data: dict) -> InterbankLoanPayableRecord:
        """Create InterbankLoanPayableRecord from dictionary (for RabbitMQ consumption)"""
        return InterbankLoanPayableRecord(
            source_table='interbankLoanPayable',
            timestamp_column_value=record_data.get('reportingDate', ''),
            reportingDate=record_data.get('reportingDate', ''),
            lenderName=record_data.get('lenderName'),
            accountNumber=record_data.get('accountNumber'),
            lenderCountry=record_data.get('lenderCountry'),
            borrowingType=record_data.get('borrowingType'),
            transactionDate=datetime.fromisoformat(record_data.get('transactionDate')).date() if record_data.get('transactionDate') else None,
            disbursementDate=datetime.fromisoformat(record_data.get('disbursementDate')).date() if record_data.get('disbursementDate') else None,
            maturityDate=datetime.fromisoformat(record_data.get('maturityDate')).date() if record_data.get('maturityDate') else None,
            currency=record_data.get('currency'),
            orgAmountOpening=Decimal(str(record_data.get('orgAmountOpening'))) if record_data.get('orgAmountOpening') is not None else None,
            usdAmountOpening=Decimal(str(record_data.get('usdAmountOpening'))) if record_data.get('usdAmountOpening') is not None else None,
            tzsAmountOpening=Decimal(str(record_data.get('tzsAmountOpening'))) if record_data.get('tzsAmountOpening') is not None else None,
            orgAmountRepayment=Decimal(str(record_data.get('orgAmountRepayment'))) if record_data.get('orgAmountRepayment') is not None else None,
            usdAmountRepayment=Decimal(str(record_data.get('usdAmountRepayment'))) if record_data.get('usdAmountRepayment') is not None else None,
            tzsAmountRepayment=Decimal(str(record_data.get('tzsAmountRepayment'))) if record_data.get('tzsAmountRepayment') is not None else None,
            orgAmountClosing=Decimal(str(record_data.get('orgAmountClosing'))) if record_data.get('orgAmountClosing') is not None else None,
            usdAmountClosing=Decimal(str(record_data.get('usdAmountClosing'))) if record_data.get('usdAmountClosing') is not None else None,
            tzsAmountClosing=Decimal(str(record_data.get('tzsAmountClosing'))) if record_data.get('tzsAmountClosing') is not None else None,
            tenureDays=int(record_data.get('tenureDays')) if record_data.get('tenureDays') is not None else None,
            annualInterestRate=Decimal(str(record_data.get('annualInterestRate'))) if record_data.get('annualInterestRate') is not None else None,
            interestRateType=record_data.get('interestRateType'),
            original_timestamp=datetime.now().isoformat()
        )
    
    def insert_to_postgres(self, record: InterbankLoanPayableRecord, pg_cursor) -> None:
        """Insert Interbank Loan Payable record to PostgreSQL"""
        query = """
        INSERT INTO "interbankLoanPayable" (
            "reportingDate", "lenderName", "accountNumber", "lenderCountry", "borrowingType",
            "transactionDate", "disbursementDate", "maturityDate", "currency",
            "orgAmountOpening", "usdAmountOpening", "tzsAmountOpening",
            "orgAmountRepayment", "usdAmountRepayment", "tzsAmountRepayment",
            "orgAmountClosing", "usdAmountClosing", "tzsAmountClosing",
            "tenureDays", "annualInterestRate", "interestRateType"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        pg_cursor.execute(query, (
            record.reportingDate, record.lenderName, record.accountNumber, record.lenderCountry,
            record.borrowingType, record.transactionDate, record.disbursementDate, record.maturityDate,
            record.currency, record.orgAmountOpening, record.usdAmountOpening, record.tzsAmountOpening,
            record.orgAmountRepayment, record.usdAmountRepayment, record.tzsAmountRepayment,
            record.orgAmountClosing, record.usdAmountClosing, record.tzsAmountClosing,
            record.tenureDays, record.annualInterestRate, record.interestRateType
        ))
    
    def get_upsert_query(self) -> str:
        """Get upsert query for interbank loan payable"""
        return self.insert_to_postgres.__doc__  # Same as insert for now
    
    def validate_record(self, record: InterbankLoanPayableRecord) -> bool:
        """Validate Interbank Loan Payable record"""
        if not super().validate_record(record):
            return False
        
        # Basic validations
        if not record.accountNumber:
            return False
            
        return True