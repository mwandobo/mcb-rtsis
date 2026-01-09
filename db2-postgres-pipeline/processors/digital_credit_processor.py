"""
Digital credit processor - Based on digitalCredit.sql
"""

from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime
from .base import BaseProcessor, BaseRecord

@dataclass
class DigitalCreditRecord(BaseRecord):
    """Digital credit record structure - Based on digitalCredit.sql"""
    reporting_date: str
    customer_name: str
    gender: Optional[str]
    disability_status: Optional[str]
    customer_identification_number: str
    institution_code: str
    branch_code: str
    services_facilitator: str
    product_name: str
    tzs_loan_disbursed_amount: Optional[float]
    loan_disbursement_date: str
    tzs_loan_balance: Optional[float]
    maturity_date: str
    loan_id: str
    last_deposit_date: Optional[str]
    last_deposit_amount: Optional[float]
    payments_installment: Optional[int]
    repayments_frequency: str
    loan_amotization_type: str
    cycle_number: Optional[int]
    loan_amount_paid: Optional[float]
    deliquence_date: Optional[str]
    restructuring_date: Optional[str]
    interest_rate: Optional[float]
    past_due_days: Optional[int]
    past_due_amount: Optional[float]
    currency: str
    org_accrued_interest: Optional[float]
    tzs_accrued_interest: Optional[float]
    usd_accrued_interest: Optional[float]
    asset_classification: str
    allowance_probable_loss: Optional[float]
    bot_provision: Optional[float]
    interest_suspended: Optional[float]
    retry_count: int = 0

class DigitalCreditProcessor(BaseProcessor):
    """Processor for digital credit data - Based on digitalCredit.sql"""
    
    def process_record(self, raw_data: Tuple, table_name: str) -> DigitalCreditRecord:
        """Convert raw DB2 data to DigitalCreditRecord"""
        # Handle None values safely
        def safe_str(value):
            return str(value).strip() if value is not None else None
        
        def safe_int(value):
            try:
                return int(value) if value is not None else None
            except (ValueError, TypeError):
                return None
        
        def safe_float(value):
            try:
                return float(value) if value is not None else None
            except (ValueError, TypeError):
                return None
        
        return DigitalCreditRecord(
            source_table=table_name,
            timestamp_column_value=safe_str(raw_data[11]),  # loanDisbursementDate for tracking
            reporting_date=safe_str(raw_data[0]),
            customer_name=safe_str(raw_data[1]),
            gender=safe_str(raw_data[2]),
            disability_status=safe_str(raw_data[3]),
            customer_identification_number=safe_str(raw_data[4]),
            institution_code=safe_str(raw_data[5]),
            branch_code=safe_str(raw_data[6]),
            services_facilitator=safe_str(raw_data[7]),
            product_name=safe_str(raw_data[8]),
            tzs_loan_disbursed_amount=safe_float(raw_data[9]),
            loan_disbursement_date=safe_str(raw_data[10]),
            tzs_loan_balance=safe_float(raw_data[11]),
            maturity_date=safe_str(raw_data[12]),
            loan_id=safe_str(raw_data[13]),
            last_deposit_date=safe_str(raw_data[14]),
            last_deposit_amount=safe_float(raw_data[15]),
            payments_installment=safe_int(raw_data[16]),
            repayments_frequency=safe_str(raw_data[17]),
            loan_amotization_type=safe_str(raw_data[18]),
            cycle_number=safe_int(raw_data[19]),
            loan_amount_paid=safe_float(raw_data[20]),
            deliquence_date=safe_str(raw_data[21]),
            restructuring_date=safe_str(raw_data[22]),
            interest_rate=safe_float(raw_data[23]),
            past_due_days=safe_int(raw_data[24]),
            past_due_amount=safe_float(raw_data[25]),
            currency=safe_str(raw_data[26]),
            org_accrued_interest=safe_float(raw_data[27]),
            tzs_accrued_interest=safe_float(raw_data[28]),
            usd_accrued_interest=safe_float(raw_data[29]),
            asset_classification=safe_str(raw_data[30]),
            allowance_probable_loss=safe_float(raw_data[31]),
            bot_provision=safe_float(raw_data[32]),
            interest_suspended=safe_float(raw_data[33]),
            original_timestamp=datetime.now().isoformat()
        )
    
    def insert_to_postgres(self, record: DigitalCreditRecord, pg_cursor) -> None:
        """Insert digital credit record to PostgreSQL"""
        query = self.get_upsert_query()
        
        pg_cursor.execute(query, (
            record.reporting_date,
            record.customer_name,
            record.gender,
            record.disability_status,
            record.customer_identification_number,
            record.institution_code,
            record.branch_code,
            record.services_facilitator,
            record.product_name,
            record.tzs_loan_disbursed_amount,
            record.loan_disbursement_date,
            record.tzs_loan_balance,
            record.maturity_date,
            record.loan_id,
            record.last_deposit_date,
            record.last_deposit_amount,
            record.payments_installment,
            record.repayments_frequency,
            record.loan_amotization_type,
            record.cycle_number,
            record.loan_amount_paid,
            record.deliquence_date,
            record.restructuring_date,
            record.interest_rate,
            record.past_due_days,
            record.past_due_amount,
            record.currency,
            record.org_accrued_interest,
            record.tzs_accrued_interest,
            record.usd_accrued_interest,
            record.asset_classification,
            record.allowance_probable_loss,
            record.bot_provision,
            record.interest_suspended
        ))
    
    def get_upsert_query(self) -> str:
        """Get upsert query for digital credit with duplicate handling"""
        return """
        INSERT INTO "digitalCredit" (
            "reportingDate", "customerName", "gender", "disabilityStatus", "customerIdentificationNumber",
            "institutionCode", "branchCode", "servicesFacilitator", "productName", "tzsLoanDisbursedAmount",
            "loanDisbursementDate", "tzsLoanBalance", "maturityDate", "loanId", "lastDepositDate",
            "lastDepositAmount", "paymentsInstallment", "repaymentsFrequency", "loanAmotizationType",
            "cycleNumber", "loanAmountPaid", "deliquenceDate", "restructuringDate", "interestRate",
            "pastDueDays", "pastDueAmount", "currency", "orgAccruedInterest", "tzsAccruedInterest",
            "usdAccruedInterest", "assetClassification", "allowanceProbableLoss", "botProvision",
            "interestSuspended"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT ("loanId") DO UPDATE SET
            "reportingDate" = EXCLUDED."reportingDate",
            "customerName" = EXCLUDED."customerName",
            "gender" = EXCLUDED."gender",
            "disabilityStatus" = EXCLUDED."disabilityStatus",
            "customerIdentificationNumber" = EXCLUDED."customerIdentificationNumber",
            "institutionCode" = EXCLUDED."institutionCode",
            "branchCode" = EXCLUDED."branchCode",
            "servicesFacilitator" = EXCLUDED."servicesFacilitator",
            "productName" = EXCLUDED."productName",
            "tzsLoanDisbursedAmount" = EXCLUDED."tzsLoanDisbursedAmount",
            "loanDisbursementDate" = EXCLUDED."loanDisbursementDate",
            "tzsLoanBalance" = EXCLUDED."tzsLoanBalance",
            "maturityDate" = EXCLUDED."maturityDate",
            "lastDepositDate" = EXCLUDED."lastDepositDate",
            "lastDepositAmount" = EXCLUDED."lastDepositAmount",
            "paymentsInstallment" = EXCLUDED."paymentsInstallment",
            "repaymentsFrequency" = EXCLUDED."repaymentsFrequency",
            "loanAmotizationType" = EXCLUDED."loanAmotizationType",
            "cycleNumber" = EXCLUDED."cycleNumber",
            "loanAmountPaid" = EXCLUDED."loanAmountPaid",
            "deliquenceDate" = EXCLUDED."deliquenceDate",
            "restructuringDate" = EXCLUDED."restructuringDate",
            "interestRate" = EXCLUDED."interestRate",
            "pastDueDays" = EXCLUDED."pastDueDays",
            "pastDueAmount" = EXCLUDED."pastDueAmount",
            "currency" = EXCLUDED."currency",
            "orgAccruedInterest" = EXCLUDED."orgAccruedInterest",
            "tzsAccruedInterest" = EXCLUDED."tzsAccruedInterest",
            "usdAccruedInterest" = EXCLUDED."usdAccruedInterest",
            "assetClassification" = EXCLUDED."assetClassification",
            "allowanceProbableLoss" = EXCLUDED."allowanceProbableLoss",
            "botProvision" = EXCLUDED."botProvision",
            "interestSuspended" = EXCLUDED."interestSuspended"
        """
    
    def validate_record(self, record: DigitalCreditRecord) -> bool:
        """Validate digital credit record"""
        if not super().validate_record(record):
            return False
        
        # Digital credit-specific validations
        if not record.customer_name:
            return False
        if not record.customer_identification_number:
            return False
        if not record.loan_id:
            return False
        if not record.services_facilitator:
            return False
        if not record.product_name:
            return False
            
        return True
    
    def transform_data(self, raw_data: Tuple) -> dict:
        """Transform digital credit data"""
        # Add any digital credit-specific transformations here
        return {
            'services_facilitator_normalized': str(raw_data[7]).strip().title() if raw_data[7] else 'Internal Facilitated',
            'currency_normalized': str(raw_data[26]).strip().upper() if raw_data[26] else 'TZS',
            'asset_classification_normalized': str(raw_data[30]).strip().title() if raw_data[30] else 'Current'
        }