"""
Inter-bank loan receivable processor - Based on inter-bank-loan-receivable.sql
"""

from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime
from .base import BaseProcessor, BaseRecord

@dataclass
class InterBankLoanReceivableRecord(BaseRecord):
    """Inter-bank loan receivable record structure"""
    reporting_date: str
    customer_identification_number: Optional[str]
    account_number: Optional[str]
    client_name: Optional[str]
    borrower_country: Optional[str]
    rating_status: Optional[str]
    cr_rating_borrower: Optional[str]
    grades_unrated_banks: Optional[str]
    gender: Optional[str]
    disability: Optional[str]
    client_type: Optional[str]
    client_sub_type: Optional[str]
    group_name: Optional[str]
    group_code: Optional[str]
    related_party: Optional[str]
    relationship_category: Optional[str]
    loan_number: Optional[str]
    loan_type: Optional[str]
    loan_economic_activity: Optional[str]
    loan_phase: Optional[str]
    transfer_status: Optional[str]
    purpose_mortgage: Optional[str]
    purpose_other_loans: Optional[str]
    source_fund_mortgage: Optional[str]
    amortization_type: Optional[str]
    branch_code: Optional[str]
    loan_officer: Optional[str]
    loan_supervisor: Optional[str]
    group_village_number: Optional[str]
    cycle_number: Optional[str]
    loan_installment: Optional[int]
    repayment_frequency: Optional[str]
    currency: Optional[str]
    contract_date: Optional[str]
    org_sanctioned_amount: Optional[float]
    usd_sanctioned_amount: Optional[float]
    tzs_sanctioned_amount: Optional[float]
    org_disbursed_amount: Optional[float]
    usd_disbursed_amount: Optional[float]
    tzs_disbursed_amount: Optional[float]
    disbursement_date: Optional[str]
    maturity_date: Optional[str]
    real_end_date: Optional[str]
    org_outstanding_principal_amount: Optional[float]
    usd_outstanding_principal_amount: Optional[float]
    tzs_outstanding_principal_amount: Optional[float]
    org_installment_amount: Optional[float]
    usd_installment_amount: Optional[float]
    tzs_installment_amount: Optional[float]
    loan_installment_paid: Optional[int]
    grace_period_payment_principal: Optional[int]
    prime_lending_rate: Optional[float]
    interest_pricing_method: Optional[str]
    annual_interest_rate: Optional[float]
    effective_annual_interest_rate: Optional[float]
    loan_flag_type: Optional[str]
    restructuring_date: Optional[str]
    past_due_days: Optional[int]
    past_due_amount: Optional[float]
    internal_risk_group: Optional[str]
    org_accrued_interest_amount: Optional[float]
    usd_accrued_interest_amount: Optional[float]
    tzs_accrued_interest_amount: Optional[float]
    org_penalty_charged_amount: Optional[float]
    usd_penalty_charged_amount: Optional[float]
    tzs_penalty_charged_amount: Optional[float]
    org_penalty_paid_amount: Optional[float]
    usd_penalty_paid_amount: Optional[float]
    tzs_penalty_paid_amount: Optional[float]
    org_loan_fees_charged_amount: Optional[float]
    usd_loan_fees_charged_amount: Optional[float]
    tzs_loan_fees_charged_amount: Optional[float]
    org_loan_fees_paid_amount: Optional[float]
    usd_loan_fees_paid_amount: Optional[float]
    tzs_loan_fees_paid_amount: Optional[float]
    org_tot_monthly_payment_amount: Optional[float]
    usd_tot_monthly_payment_amount: Optional[float]
    tzs_tot_monthly_payment_amount: Optional[float]
    sector_sna_classification: Optional[str]
    asset_classification_category: Optional[str]
    neg_status_contract: Optional[str]
    customer_role: Optional[str]
    allowance_probable_loss: Optional[float]
    bot_provision: Optional[float]
    trading_intent: Optional[str]
    org_suspended_interest: Optional[float]
    usd_suspended_interest: Optional[float]
    tzs_suspended_interest: Optional[float]
    retry_count: int = 0

class InterBankLoanReceivableProcessor(BaseProcessor):
    """Processor for inter-bank loan receivable data"""
    
    def process_record(self, raw_data: Tuple, table_name: str) -> InterBankLoanReceivableRecord:
        """Convert raw DB2 data to InterBankLoanReceivableRecord"""
        # Handle None values safely
        def safe_str(value):
            return str(value).strip() if value is not None else None
        
        def safe_float(value):
            try:
                return float(value) if value is not None else None
            except (ValueError, TypeError):
                return None
        
        def safe_int(value):
            try:
                return int(value) if value is not None else None
            except (ValueError, TypeError):
                return None
        
        return InterBankLoanReceivableRecord(
            source_table=table_name,
            timestamp_column_value=safe_str(raw_data[0]),  # reportingDate for tracking
            reporting_date=safe_str(raw_data[0]),
            customer_identification_number=safe_str(raw_data[1]),
            account_number=safe_str(raw_data[2]),
            client_name=safe_str(raw_data[3]),
            borrower_country=safe_str(raw_data[4]),
            rating_status=safe_str(raw_data[5]),
            cr_rating_borrower=safe_str(raw_data[6]),
            grades_unrated_banks=safe_str(raw_data[7]),
            gender=safe_str(raw_data[8]),
            disability=safe_str(raw_data[9]),
            client_type=safe_str(raw_data[10]),
            client_sub_type=safe_str(raw_data[11]),
            group_name=safe_str(raw_data[12]),
            group_code=safe_str(raw_data[13]),
            related_party=safe_str(raw_data[14]),
            relationship_category=safe_str(raw_data[15]),
            loan_number=safe_str(raw_data[16]),
            loan_type=safe_str(raw_data[17]),
            loan_economic_activity=safe_str(raw_data[18]),
            loan_phase=safe_str(raw_data[19]),
            transfer_status=safe_str(raw_data[20]),
            purpose_mortgage=safe_str(raw_data[21]),
            purpose_other_loans=safe_str(raw_data[22]),
            source_fund_mortgage=safe_str(raw_data[23]),
            amortization_type=safe_str(raw_data[24]),
            branch_code=safe_str(raw_data[25]),
            loan_officer=safe_str(raw_data[26]),
            loan_supervisor=safe_str(raw_data[27]),
            group_village_number=safe_str(raw_data[28]),
            cycle_number=safe_str(raw_data[29]),
            loan_installment=safe_int(raw_data[30]),
            repayment_frequency=safe_str(raw_data[31]),
            currency=safe_str(raw_data[32]),
            contract_date=safe_str(raw_data[33]),
            org_sanctioned_amount=safe_float(raw_data[34]),
            usd_sanctioned_amount=safe_float(raw_data[35]),
            tzs_sanctioned_amount=safe_float(raw_data[36]),
            org_disbursed_amount=safe_float(raw_data[37]),
            usd_disbursed_amount=safe_float(raw_data[38]),
            tzs_disbursed_amount=safe_float(raw_data[39]),
            disbursement_date=safe_str(raw_data[40]),
            maturity_date=safe_str(raw_data[41]),
            real_end_date=safe_str(raw_data[42]),
            org_outstanding_principal_amount=safe_float(raw_data[43]),
            usd_outstanding_principal_amount=safe_float(raw_data[44]),
            tzs_outstanding_principal_amount=safe_float(raw_data[45]),
            org_installment_amount=safe_float(raw_data[46]),
            usd_installment_amount=safe_float(raw_data[47]),
            tzs_installment_amount=safe_float(raw_data[48]),
            loan_installment_paid=safe_int(raw_data[49]),
            grace_period_payment_principal=safe_int(raw_data[50]),
            prime_lending_rate=safe_float(raw_data[51]),
            interest_pricing_method=safe_str(raw_data[52]),
            annual_interest_rate=safe_float(raw_data[53]),
            effective_annual_interest_rate=safe_float(raw_data[54]),
            loan_flag_type=safe_str(raw_data[55]),
            restructuring_date=safe_str(raw_data[56]),
            past_due_days=safe_int(raw_data[57]),
            past_due_amount=safe_float(raw_data[58]),
            internal_risk_group=safe_str(raw_data[59]),
            org_accrued_interest_amount=safe_float(raw_data[60]),
            usd_accrued_interest_amount=safe_float(raw_data[61]),
            tzs_accrued_interest_amount=safe_float(raw_data[62]),
            org_penalty_charged_amount=safe_float(raw_data[63]),
            usd_penalty_charged_amount=safe_float(raw_data[64]),
            tzs_penalty_charged_amount=safe_float(raw_data[65]),
            org_penalty_paid_amount=safe_float(raw_data[66]),
            usd_penalty_paid_amount=safe_float(raw_data[67]),
            tzs_penalty_paid_amount=safe_float(raw_data[68]),
            org_loan_fees_charged_amount=safe_float(raw_data[69]),
            usd_loan_fees_charged_amount=safe_float(raw_data[70]),
            tzs_loan_fees_charged_amount=safe_float(raw_data[71]),
            org_loan_fees_paid_amount=safe_float(raw_data[72]),
            usd_loan_fees_paid_amount=safe_float(raw_data[73]),
            tzs_loan_fees_paid_amount=safe_float(raw_data[74]),
            org_tot_monthly_payment_amount=safe_float(raw_data[75]),
            usd_tot_monthly_payment_amount=safe_float(raw_data[76]),
            tzs_tot_monthly_payment_amount=safe_float(raw_data[77]),
            sector_sna_classification=safe_str(raw_data[78]),
            asset_classification_category=safe_str(raw_data[79]),
            neg_status_contract=safe_str(raw_data[80]),
            customer_role=safe_str(raw_data[81]),
            allowance_probable_loss=safe_float(raw_data[82]),
            bot_provision=safe_float(raw_data[83]),
            trading_intent=safe_str(raw_data[84]),
            org_suspended_interest=safe_float(raw_data[85]),
            usd_suspended_interest=safe_float(raw_data[86]),
            tzs_suspended_interest=safe_float(raw_data[87]),
            original_timestamp=datetime.now().isoformat()
        )
    
    def insert_to_postgres(self, record: InterBankLoanReceivableRecord, pg_cursor) -> None:
        """Insert inter-bank loan receivable record to PostgreSQL"""
        query = self.get_insert_query()
        
        pg_cursor.execute(query, (
            record.reporting_date, record.customer_identification_number, record.account_number,
            record.client_name, record.borrower_country, record.rating_status, record.cr_rating_borrower,
            record.grades_unrated_banks, record.gender, record.disability, record.client_type,
            record.client_sub_type, record.group_name, record.group_code, record.related_party,
            record.relationship_category, record.loan_number, record.loan_type, record.loan_economic_activity,
            record.loan_phase, record.transfer_status, record.purpose_mortgage, record.purpose_other_loans,
            record.source_fund_mortgage, record.amortization_type, record.branch_code, record.loan_officer,
            record.loan_supervisor, record.group_village_number, record.cycle_number, record.loan_installment,
            record.repayment_frequency, record.currency, record.contract_date, record.org_sanctioned_amount,
            record.usd_sanctioned_amount, record.tzs_sanctioned_amount, record.org_disbursed_amount,
            record.usd_disbursed_amount, record.tzs_disbursed_amount, record.disbursement_date,
            record.maturity_date, record.real_end_date, record.org_outstanding_principal_amount,
            record.usd_outstanding_principal_amount, record.tzs_outstanding_principal_amount,
            record.org_installment_amount, record.usd_installment_amount, record.tzs_installment_amount,
            record.loan_installment_paid, record.grace_period_payment_principal, record.prime_lending_rate,
            record.interest_pricing_method, record.annual_interest_rate, record.effective_annual_interest_rate,
            record.loan_flag_type, record.restructuring_date, record.past_due_days, record.past_due_amount,
            record.internal_risk_group, record.org_accrued_interest_amount, record.usd_accrued_interest_amount,
            record.tzs_accrued_interest_amount, record.org_penalty_charged_amount, record.usd_penalty_charged_amount,
            record.tzs_penalty_charged_amount, record.org_penalty_paid_amount, record.usd_penalty_paid_amount,
            record.tzs_penalty_paid_amount, record.org_loan_fees_charged_amount, record.usd_loan_fees_charged_amount,
            record.tzs_loan_fees_charged_amount, record.org_loan_fees_paid_amount, record.usd_loan_fees_paid_amount,
            record.tzs_loan_fees_paid_amount, record.org_tot_monthly_payment_amount, record.usd_tot_monthly_payment_amount,
            record.tzs_tot_monthly_payment_amount, record.sector_sna_classification, record.asset_classification_category,
            record.neg_status_contract, record.customer_role, record.allowance_probable_loss, record.bot_provision,
            record.trading_intent, record.org_suspended_interest, record.usd_suspended_interest, record.tzs_suspended_interest
        ))
    
    def get_insert_query(self) -> str:
        """Get insert query for inter-bank loan receivable"""
        return """
        INSERT INTO "interBankLoanReceivable" (
            "reportingDate", "customerIdentificationNumber", "accountNumber", "clientName", "borrowerCountry",
            "ratingStatus", "crRatingBorrower", "gradesUnratedBanks", "gender", "disability", "clientType",
            "clientSubType", "groupName", "groupCode", "relatedParty", "relationshipCategory", "loanNumber",
            "loanType", "loanEconomicActivity", "loanPhase", "transferStatus", "purposeMortgage", "purposeOtherLoans",
            "sourceFundMortgage", "amortizationType", "branchCode", "loanOfficer", "loanSupervisor",
            "groupVillageNumber", "cycleNumber", "loanInstallment", "repaymentFrequency", "currency",
            "contractDate", "orgSanctionedAmount", "usdSanctionedAmount", "tzsSanctionedAmount",
            "orgDisbursedAmount", "usdDisbursedAmount", "tzsDisbursedAmount", "disbursementDate",
            "maturityDate", "realEndDate", "orgOutstandingPrincipalAmount", "usdOutstandingPrincipalAmount",
            "tzsOutstandingPrincipalAmount", "orgInstallmentAmount", "usdInstallmentAmount", "tzsInstallmentAmount",
            "loanInstallmentPaid", "gracePeriodPaymentPrincipal", "primeLendingRate", "interestPricingMethod",
            "annualInterestRate", "effectiveAnnualInterestRate", "loanFlagType", "restructuringDate",
            "pastDueDays", "pastDueAmount", "internalRiskGroup", "orgAccruedInterestAmount",
            "usdAccruedInterestAmount", "tzsAccruedInterestAmount", "orgPenaltyChargedAmount",
            "usdPenaltyChargedAmount", "tzsPenaltyChargedAmount", "orgPenaltyPaidAmount", "usdPenaltyPaidAmount",
            "tzsPenaltyPaidAmount", "orgLoanFeesChargedAmount", "usdLoanFeesChargedAmount", "tzsLoanFeesChargedAmount",
            "orgLoanFeesPaidAmount", "usdLoanFeesPaidAmount", "tzsLoanFeesPaidAmount", "orgTotMonthlyPaymentAmount",
            "usdTotMonthlyPaymentAmount", "tzsTotMonthlyPaymentAmount", "sectorSnaClassification",
            "assetClassificationCategory", "negStatusContract", "customerRole", "allowanceProbableLoss",
            "botProvision", "tradingIntent", "orgSuspendedInterest", "usdSuspendedInterest", "tzsSuspendedInterest"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
    
    def get_upsert_query(self) -> str:
        """Get upsert query for inter-bank loan receivable (use loan number as unique key)"""
        return """
        INSERT INTO "interBankLoanReceivable" (
            "reportingDate", "customerIdentificationNumber", "accountNumber", "clientName", "borrowerCountry",
            "ratingStatus", "crRatingBorrower", "gradesUnratedBanks", "gender", "disability", "clientType",
            "clientSubType", "groupName", "groupCode", "relatedParty", "relationshipCategory", "loanNumber",
            "loanType", "loanEconomicActivity", "loanPhase", "transferStatus", "purposeMortgage", "purposeOtherLoans",
            "sourceFundMortgage", "amortizationType", "branchCode", "loanOfficer", "loanSupervisor",
            "groupVillageNumber", "cycleNumber", "loanInstallment", "repaymentFrequency", "currency",
            "contractDate", "orgSanctionedAmount", "usdSanctionedAmount", "tzsSanctionedAmount",
            "orgDisbursedAmount", "usdDisbursedAmount", "tzsDisbursedAmount", "disbursementDate",
            "maturityDate", "realEndDate", "orgOutstandingPrincipalAmount", "usdOutstandingPrincipalAmount",
            "tzsOutstandingPrincipalAmount", "orgInstallmentAmount", "usdInstallmentAmount", "tzsInstallmentAmount",
            "loanInstallmentPaid", "gracePeriodPaymentPrincipal", "primeLendingRate", "interestPricingMethod",
            "annualInterestRate", "effectiveAnnualInterestRate", "loanFlagType", "restructuringDate",
            "pastDueDays", "pastDueAmount", "internalRiskGroup", "orgAccruedInterestAmount",
            "usdAccruedInterestAmount", "tzsAccruedInterestAmount", "orgPenaltyChargedAmount",
            "usdPenaltyChargedAmount", "tzsPenaltyChargedAmount", "orgPenaltyPaidAmount", "usdPenaltyPaidAmount",
            "tzsPenaltyPaidAmount", "orgLoanFeesChargedAmount", "usdLoanFeesChargedAmount", "tzsLoanFeesChargedAmount",
            "orgLoanFeesPaidAmount", "usdLoanFeesPaidAmount", "tzsLoanFeesPaidAmount", "orgTotMonthlyPaymentAmount",
            "usdTotMonthlyPaymentAmount", "tzsTotMonthlyPaymentAmount", "sectorSnaClassification",
            "assetClassificationCategory", "negStatusContract", "customerRole", "allowanceProbableLoss",
            "botProvision", "tradingIntent", "orgSuspendedInterest", "usdSuspendedInterest", "tzsSuspendedInterest"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
    
    def validate_record(self, record: InterBankLoanReceivableRecord) -> bool:
        """Validate inter-bank loan receivable record"""
        if not super().validate_record(record):
            return False
        
        # Inter-bank loan receivable-specific validations
        if not record.customer_identification_number:
            return False
        if not record.loan_number:
            return False
        if not record.currency:
            return False
            
        return True
    
    def transform_data(self, raw_data: Tuple) -> dict:
        """Transform inter-bank loan receivable data"""
        # Add any inter-bank loan receivable-specific transformations here
        return {
            'customer_id_normalized': str(raw_data[1]).strip() if raw_data[1] else '',
            'currency_normalized': str(raw_data[32]).strip().upper() if raw_data[32] else 'TZS',
            'loan_type_normalized': str(raw_data[17]).strip().title() if raw_data[17] else 'Unknown'
        }