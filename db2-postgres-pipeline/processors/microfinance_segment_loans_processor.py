"""
Microfinance segment loans processor - Based on microfinance-segment-loans.sql
"""

from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime
from .base import BaseProcessor, BaseRecord

@dataclass
class MicrofinanceSegmentLoansRecord(BaseRecord):
    """Microfinance segment loans record structure - Based on microfinance-segment-loans.sql"""
    reporting_date: str
    customer_identification_number: str
    account_number: str
    client_name: str
    client_type: str
    gender: str
    age: Optional[int]
    disability_status: Optional[str]
    loan_number: str
    loan_industry_classification: str
    loan_sub_industry: str
    microfinance_loans_type: str
    amortization_type: str
    branch_code: str
    loan_officer: str
    loan_supervisor: str
    group_village_number: Optional[str]
    cycle_number: int
    currency: str
    org_sanctioned_amount: Optional[float]
    usd_sanctioned_amount: Optional[float]
    tzs_sanctioned_amount: Optional[float]
    org_disbursed_amount: Optional[float]
    usd_disbursed_amount: Optional[float]
    tzs_disbursed_amount: Optional[float]
    disbursement_date: str
    maturity_date: str
    restructuring_date: Optional[str]
    written_off_amount: Optional[float]
    agreed_loan_installments: Optional[int]
    repayment_frequency: str
    org_outstanding_principal_amount: Optional[float]
    usd_outstanding_principal_amount: Optional[float]
    tzs_outstanding_principal_amount: Optional[float]
    loan_installment_paid: Optional[int]
    grace_period_payment_principal: int
    prime_lending_rate: Optional[float]
    annual_interest_rate: Optional[float]
    effective_annual_interest_rate: Optional[float]
    first_installment_payment_date: Optional[str]
    loan_flag_type: str
    past_due_days: int
    past_due_amount: Optional[float]
    org_accrued_interest_amount: Optional[float]
    usd_accrued_interest_amount: Optional[float]
    tzs_accrued_interest_amount: Optional[float]
    asset_classification_category: str
    allowance_probable_loss: Optional[float]
    bot_provision: Optional[float]
    retry_count: int = 0

class MicrofinanceSegmentLoansProcessor(BaseProcessor):
    """Processor for microfinance segment loans data - Based on microfinance-segment-loans.sql"""
    
    def process_record(self, raw_data: Tuple, table_name: str) -> MicrofinanceSegmentLoansRecord:
        """Convert raw DB2 data to MicrofinanceSegmentLoansRecord"""
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
        
        return MicrofinanceSegmentLoansRecord(
            source_table=table_name,
            timestamp_column_value=safe_str(raw_data[26]),  # disbursementDate for tracking
            reporting_date=safe_str(raw_data[0]),
            customer_identification_number=safe_str(raw_data[1]),
            account_number=safe_str(raw_data[2]),
            client_name=safe_str(raw_data[3]),
            client_type=safe_str(raw_data[4]),
            gender=safe_str(raw_data[5]),
            age=safe_int(raw_data[6]),
            disability_status=safe_str(raw_data[7]),
            loan_number=safe_str(raw_data[8]),
            loan_industry_classification=safe_str(raw_data[9]),
            loan_sub_industry=safe_str(raw_data[10]),
            microfinance_loans_type=safe_str(raw_data[11]),
            amortization_type=safe_str(raw_data[12]),
            branch_code=safe_str(raw_data[13]),
            loan_officer=safe_str(raw_data[14]),
            loan_supervisor=safe_str(raw_data[15]),
            group_village_number=safe_str(raw_data[16]),
            cycle_number=safe_int(raw_data[17]) or 1,
            currency=safe_str(raw_data[18]),
            org_sanctioned_amount=safe_float(raw_data[19]),
            usd_sanctioned_amount=safe_float(raw_data[20]),
            tzs_sanctioned_amount=safe_float(raw_data[21]),
            org_disbursed_amount=safe_float(raw_data[22]),
            usd_disbursed_amount=safe_float(raw_data[23]),
            tzs_disbursed_amount=safe_float(raw_data[24]),
            disbursement_date=safe_str(raw_data[25]),
            maturity_date=safe_str(raw_data[26]),
            restructuring_date=safe_str(raw_data[27]),
            written_off_amount=safe_float(raw_data[28]),
            agreed_loan_installments=safe_int(raw_data[29]),
            repayment_frequency=safe_str(raw_data[30]),
            org_outstanding_principal_amount=safe_float(raw_data[31]),
            usd_outstanding_principal_amount=safe_float(raw_data[32]),
            tzs_outstanding_principal_amount=safe_float(raw_data[33]),
            loan_installment_paid=safe_int(raw_data[34]),
            grace_period_payment_principal=safe_int(raw_data[35]) or 0,
            prime_lending_rate=safe_float(raw_data[36]),
            annual_interest_rate=safe_float(raw_data[37]),
            effective_annual_interest_rate=safe_float(raw_data[38]),
            first_installment_payment_date=safe_str(raw_data[39]),
            loan_flag_type=safe_str(raw_data[40]),
            past_due_days=safe_int(raw_data[41]) or 0,
            past_due_amount=safe_float(raw_data[42]),
            org_accrued_interest_amount=safe_float(raw_data[43]),
            usd_accrued_interest_amount=safe_float(raw_data[44]),
            tzs_accrued_interest_amount=safe_float(raw_data[45]),
            asset_classification_category=safe_str(raw_data[46]),
            allowance_probable_loss=safe_float(raw_data[47]),
            bot_provision=safe_float(raw_data[48]),
            original_timestamp=datetime.now().isoformat()
        )
    
    def insert_to_postgres(self, record: MicrofinanceSegmentLoansRecord, pg_cursor) -> None:
        """Insert microfinance segment loans record to PostgreSQL"""
        query = self.get_upsert_query()
        
        pg_cursor.execute(query, (
            record.reporting_date,
            record.customer_identification_number,
            record.account_number,
            record.client_name,
            record.client_type,
            record.gender,
            record.age,
            record.disability_status,
            record.loan_number,
            record.loan_industry_classification,
            record.loan_sub_industry,
            record.microfinance_loans_type,
            record.amortization_type,
            record.branch_code,
            record.loan_officer,
            record.loan_supervisor,
            record.group_village_number,
            record.cycle_number,
            record.currency,
            record.org_sanctioned_amount,
            record.usd_sanctioned_amount,
            record.tzs_sanctioned_amount,
            record.org_disbursed_amount,
            record.usd_disbursed_amount,
            record.tzs_disbursed_amount,
            record.disbursement_date,
            record.maturity_date,
            record.restructuring_date,
            record.written_off_amount,
            record.agreed_loan_installments,
            record.repayment_frequency,
            record.org_outstanding_principal_amount,
            record.usd_outstanding_principal_amount,
            record.tzs_outstanding_principal_amount,
            record.loan_installment_paid,
            record.grace_period_payment_principal,
            record.prime_lending_rate,
            record.annual_interest_rate,
            record.effective_annual_interest_rate,
            record.first_installment_payment_date,
            record.loan_flag_type,
            record.past_due_days,
            record.past_due_amount,
            record.org_accrued_interest_amount,
            record.usd_accrued_interest_amount,
            record.tzs_accrued_interest_amount,
            record.asset_classification_category,
            record.allowance_probable_loss,
            record.bot_provision
        ))
    
    def get_upsert_query(self) -> str:
        """Get upsert query for microfinance segment loans with duplicate handling"""
        return """
        INSERT INTO "microfinanceSegmentLoans" (
            "reportingDate", "customerIdentificationNumber", "accountNumber", "clientName", "clientType",
            "gender", "age", "disabilityStatus", "loanNumber", "loanIndustryClassification",
            "loanSubIndustry", "microfinanceLoansType", "amortizationType", "branchCode", "loanOfficer",
            "loanSupervisor", "groupVillageNumber", "cycleNumber", "currency", "orgSanctionedAmount",
            "usdSanctionedAmount", "tzsSanctionedAmount", "orgDisbursedAmount", "usdDisbursedAmount",
            "tzsDisbursedAmount", "disbursementDate", "maturityDate", "restructuringDate", "writtenOffAmount",
            "agreedLoanInstallments", "repaymentFrequency", "orgOutstandingPrincipalAmount",
            "usdOutstandingPrincipalAmount", "tzsOutstandingPrincipalAmount", "loanInstallmentPaid",
            "gracePeriodPaymentPrincipal", "primeLendingRate", "annualInterestRate", "effectiveAnnualInterestRate",
            "firstInstallmentPaymentDate", "loanFlagType", "pastDueDays", "pastDueAmount",
            "orgAccruedInterestAmount", "usdAccruedInterestAmount", "tzsAccruedInterestAmount",
            "assetClassificationCategory", "allowanceProbableLoss", "botProvision"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT ("accountNumber", "loanNumber") DO UPDATE SET
            "reportingDate" = EXCLUDED."reportingDate",
            "customerIdentificationNumber" = EXCLUDED."customerIdentificationNumber",
            "clientName" = EXCLUDED."clientName",
            "clientType" = EXCLUDED."clientType",
            "gender" = EXCLUDED."gender",
            "age" = EXCLUDED."age",
            "disabilityStatus" = EXCLUDED."disabilityStatus",
            "loanIndustryClassification" = EXCLUDED."loanIndustryClassification",
            "loanSubIndustry" = EXCLUDED."loanSubIndustry",
            "microfinanceLoansType" = EXCLUDED."microfinanceLoansType",
            "amortizationType" = EXCLUDED."amortizationType",
            "branchCode" = EXCLUDED."branchCode",
            "loanOfficer" = EXCLUDED."loanOfficer",
            "loanSupervisor" = EXCLUDED."loanSupervisor",
            "groupVillageNumber" = EXCLUDED."groupVillageNumber",
            "cycleNumber" = EXCLUDED."cycleNumber",
            "currency" = EXCLUDED."currency",
            "orgSanctionedAmount" = EXCLUDED."orgSanctionedAmount",
            "usdSanctionedAmount" = EXCLUDED."usdSanctionedAmount",
            "tzsSanctionedAmount" = EXCLUDED."tzsSanctionedAmount",
            "orgDisbursedAmount" = EXCLUDED."orgDisbursedAmount",
            "usdDisbursedAmount" = EXCLUDED."usdDisbursedAmount",
            "tzsDisbursedAmount" = EXCLUDED."tzsDisbursedAmount",
            "disbursementDate" = EXCLUDED."disbursementDate",
            "maturityDate" = EXCLUDED."maturityDate",
            "restructuringDate" = EXCLUDED."restructuringDate",
            "writtenOffAmount" = EXCLUDED."writtenOffAmount",
            "agreedLoanInstallments" = EXCLUDED."agreedLoanInstallments",
            "repaymentFrequency" = EXCLUDED."repaymentFrequency",
            "orgOutstandingPrincipalAmount" = EXCLUDED."orgOutstandingPrincipalAmount",
            "usdOutstandingPrincipalAmount" = EXCLUDED."usdOutstandingPrincipalAmount",
            "tzsOutstandingPrincipalAmount" = EXCLUDED."tzsOutstandingPrincipalAmount",
            "loanInstallmentPaid" = EXCLUDED."loanInstallmentPaid",
            "gracePeriodPaymentPrincipal" = EXCLUDED."gracePeriodPaymentPrincipal",
            "primeLendingRate" = EXCLUDED."primeLendingRate",
            "annualInterestRate" = EXCLUDED."annualInterestRate",
            "effectiveAnnualInterestRate" = EXCLUDED."effectiveAnnualInterestRate",
            "firstInstallmentPaymentDate" = EXCLUDED."firstInstallmentPaymentDate",
            "loanFlagType" = EXCLUDED."loanFlagType",
            "pastDueDays" = EXCLUDED."pastDueDays",
            "pastDueAmount" = EXCLUDED."pastDueAmount",
            "orgAccruedInterestAmount" = EXCLUDED."orgAccruedInterestAmount",
            "usdAccruedInterestAmount" = EXCLUDED."usdAccruedInterestAmount",
            "tzsAccruedInterestAmount" = EXCLUDED."tzsAccruedInterestAmount",
            "assetClassificationCategory" = EXCLUDED."assetClassificationCategory",
            "allowanceProbableLoss" = EXCLUDED."allowanceProbableLoss",
            "botProvision" = EXCLUDED."botProvision"
        """
    
    def validate_record(self, record: MicrofinanceSegmentLoansRecord) -> bool:
        """Validate microfinance segment loans record"""
        if not super().validate_record(record):
            return False
        
        # Microfinance segment loans-specific validations
        if not record.customer_identification_number:
            return False
        if not record.account_number:
            return False
        if not record.loan_number:
            return False
        if not record.client_name:
            return False
            
        return True
    
    def transform_data(self, raw_data: Tuple) -> dict:
        """Transform microfinance segment loans data"""
        # Add any microfinance segment loans-specific transformations here
        return {
            'client_type_normalized': str(raw_data[4]).strip().upper() if raw_data[4] else 'UNKNOWN',
            'currency_normalized': str(raw_data[18]).strip().upper() if raw_data[18] else 'TZS',
            'loan_flag_normalized': str(raw_data[40]).strip().title() if raw_data[40] else 'Non-restructured'
        }