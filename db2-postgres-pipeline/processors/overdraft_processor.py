"""
Overdraft processor
"""

from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime
from .base import BaseProcessor, BaseRecord

@dataclass
class OverdraftRecord(BaseRecord):
    """Overdraft record structure"""
    reporting_date: str
    account_number: str
    customer_identification_number: str
    client_name: str
    client_type: str
    borrower_country: str
    rating_status: Optional[str]
    cr_rating_borrower: Optional[str]
    grades_unrated_banks: Optional[str]
    group_code: Optional[str]
    related_entity_name: Optional[str]
    related_party: Optional[str]
    relationship_category: Optional[str]
    loan_product_type: str
    overdraft_economic_activity: str
    loan_phase: str
    transfer_status: str
    purpose_other_loans: str
    contract_date: str
    branch_code: str
    loan_officer: str
    loan_supervisor: Optional[str]
    currency: str
    org_sanctioned_amount: float
    usd_sanctioned_amount: Optional[float]
    tzs_sanctioned_amount: float
    org_utilised_amount: float
    usd_utilised_amount: Optional[float]
    tzs_utilised_amount: float
    org_cr_usage_last30_days_amount: float
    usd_cr_usage_last30_days_amount: Optional[float]
    tzs_cr_usage_last30_days_amount: float
    disbursement_date: str
    expiry_date: str
    real_end_date: str
    org_outstanding_amount: float
    usd_outstanding_amount: Optional[float]
    tzs_outstanding_amount: float
    latest_customer_credit_date: str
    latest_credit_amount: float
    prime_lending_rate: float
    annual_interest_rate: float
    collateral_pledged: float
    org_collateral_value: float
    usd_collateral_value: Optional[float]
    tzs_collateral_value: float
    restructured_loans: int
    past_due_days: int
    past_due_amount: float
    org_accrued_interest_amount: float
    usd_accrued_interest_amount: Optional[float]
    tzs_accrued_interest_amount: float
    org_penalty_charged_amount: float
    usd_penalty_charged_amount: Optional[float]
    tzs_penalty_charged_amount: float
    org_loan_fees_charged_amount: float
    usd_loan_fees_charged_amount: Optional[float]
    tzs_loan_fees_charged_amount: float
    org_loan_fees_paid_amount: float
    usd_loan_fees_paid_amount: Optional[float]
    tzs_loan_fees_paid_amount: float
    org_tot_monthly_payment_amount: float
    usd_tot_monthly_payment_amount: Optional[float]
    tzs_tot_monthly_payment_amount: float
    org_interest_paid_total: float
    usd_interest_paid_total: Optional[float]
    tzs_interest_paid_total: float
    asset_classification_category: str
    sector_sna_classification: str
    neg_status_contract: str
    customer_role: str
    allowance_probable_loss: float
    bot_provision: float
    retry_count: int = 0

class OverdraftProcessor(BaseProcessor):
    """Processor for overdraft data"""
    
    def process_record(self, raw_data: Tuple, table_name: str) -> OverdraftRecord:
        """Convert raw DB2 data to OverdraftRecord"""
        return OverdraftRecord(
            source_table=table_name,
            timestamp_column_value=str(raw_data[0]),  # reportingDate
            reporting_date=str(raw_data[0]),
            account_number=str(raw_data[1]),
            customer_identification_number=str(raw_data[2]),
            client_name=str(raw_data[3]),
            client_type=str(raw_data[4]),
            borrower_country=str(raw_data[5]),
            rating_status=str(raw_data[6]) if raw_data[6] else None,
            cr_rating_borrower=str(raw_data[7]) if raw_data[7] else None,
            grades_unrated_banks=str(raw_data[8]) if raw_data[8] else None,
            group_code=str(raw_data[9]) if raw_data[9] else None,
            related_entity_name=str(raw_data[10]) if raw_data[10] else None,
            related_party=str(raw_data[11]) if raw_data[11] else None,
            relationship_category=str(raw_data[12]) if raw_data[12] else None,
            loan_product_type=str(raw_data[13]),
            overdraft_economic_activity=str(raw_data[14]),
            loan_phase=str(raw_data[15]),
            transfer_status=str(raw_data[16]),
            purpose_other_loans=str(raw_data[17]),
            contract_date=str(raw_data[18]),
            branch_code=str(raw_data[19]),
            loan_officer=str(raw_data[20]),
            loan_supervisor=str(raw_data[21]) if raw_data[21] else None,
            currency=str(raw_data[22]),
            org_sanctioned_amount=float(raw_data[23]),
            usd_sanctioned_amount=float(raw_data[24]) if raw_data[24] else None,
            tzs_sanctioned_amount=float(raw_data[25]),
            org_utilised_amount=float(raw_data[26]),
            usd_utilised_amount=float(raw_data[27]) if raw_data[27] else None,
            tzs_utilised_amount=float(raw_data[28]),
            org_cr_usage_last30_days_amount=float(raw_data[29]),
            usd_cr_usage_last30_days_amount=float(raw_data[30]) if raw_data[30] else None,
            tzs_cr_usage_last30_days_amount=float(raw_data[31]),
            disbursement_date=str(raw_data[32]),
            expiry_date=str(raw_data[33]),
            real_end_date=str(raw_data[34]),
            org_outstanding_amount=float(raw_data[35]),
            usd_outstanding_amount=float(raw_data[36]) if raw_data[36] else None,
            tzs_outstanding_amount=float(raw_data[37]),
            latest_customer_credit_date=str(raw_data[38]),
            latest_credit_amount=float(raw_data[39]),
            prime_lending_rate=float(raw_data[40]),
            annual_interest_rate=float(raw_data[41]),
            collateral_pledged=float(raw_data[42]),
            org_collateral_value=float(raw_data[43]),
            usd_collateral_value=float(raw_data[44]) if raw_data[44] else None,
            tzs_collateral_value=float(raw_data[45]),
            restructured_loans=int(raw_data[46]),
            past_due_days=int(raw_data[47]),
            past_due_amount=float(raw_data[48]),
            org_accrued_interest_amount=float(raw_data[49]),
            usd_accrued_interest_amount=float(raw_data[50]) if raw_data[50] else None,
            tzs_accrued_interest_amount=float(raw_data[51]),
            org_penalty_charged_amount=float(raw_data[52]),
            usd_penalty_charged_amount=float(raw_data[53]) if raw_data[53] else None,
            tzs_penalty_charged_amount=float(raw_data[54]),
            org_loan_fees_charged_amount=float(raw_data[55]),
            usd_loan_fees_charged_amount=float(raw_data[56]) if raw_data[56] else None,
            tzs_loan_fees_charged_amount=float(raw_data[57]),
            org_loan_fees_paid_amount=float(raw_data[58]),
            usd_loan_fees_paid_amount=float(raw_data[59]) if raw_data[59] else None,
            tzs_loan_fees_paid_amount=float(raw_data[60]),
            org_tot_monthly_payment_amount=float(raw_data[61]),
            usd_tot_monthly_payment_amount=float(raw_data[62]) if raw_data[62] else None,
            tzs_tot_monthly_payment_amount=float(raw_data[63]),
            org_interest_paid_total=float(raw_data[64]),
            usd_interest_paid_total=float(raw_data[65]) if raw_data[65] else None,
            tzs_interest_paid_total=float(raw_data[66]),
            asset_classification_category=str(raw_data[67]),
            sector_sna_classification=str(raw_data[68]),
            neg_status_contract=str(raw_data[69]),
            customer_role=str(raw_data[70]),
            allowance_probable_loss=float(raw_data[71]),
            bot_provision=float(raw_data[72]),
            original_timestamp=datetime.now().isoformat()
        )
    
    def insert_to_postgres(self, record: OverdraftRecord, pg_cursor) -> None:
        """Insert Overdraft record to PostgreSQL"""
        query = self.get_upsert_query()
        
        pg_cursor.execute(query, (
            record.reporting_date,
            record.account_number,
            record.customer_identification_number,
            record.client_name,
            record.client_type,
            record.borrower_country,
            record.rating_status,
            record.cr_rating_borrower,
            record.grades_unrated_banks,
            record.group_code,
            record.related_entity_name,
            record.related_party,
            record.relationship_category,
            record.loan_product_type,
            record.overdraft_economic_activity,
            record.loan_phase,
            record.transfer_status,
            record.purpose_other_loans,
            record.contract_date,
            record.branch_code,
            record.loan_officer,
            record.loan_supervisor,
            record.currency,
            record.org_sanctioned_amount,
            record.usd_sanctioned_amount,
            record.tzs_sanctioned_amount,
            record.org_utilised_amount,
            record.usd_utilised_amount,
            record.tzs_utilised_amount,
            record.org_cr_usage_last30_days_amount,
            record.usd_cr_usage_last30_days_amount,
            record.tzs_cr_usage_last30_days_amount,
            record.disbursement_date,
            record.expiry_date,
            record.real_end_date,
            record.org_outstanding_amount,
            record.usd_outstanding_amount,
            record.tzs_outstanding_amount,
            record.latest_customer_credit_date,
            record.latest_credit_amount,
            record.prime_lending_rate,
            record.annual_interest_rate,
            record.collateral_pledged,
            record.org_collateral_value,
            record.usd_collateral_value,
            record.tzs_collateral_value,
            record.restructured_loans,
            record.past_due_days,
            record.past_due_amount,
            record.org_accrued_interest_amount,
            record.usd_accrued_interest_amount,
            record.tzs_accrued_interest_amount,
            record.org_penalty_charged_amount,
            record.usd_penalty_charged_amount,
            record.tzs_penalty_charged_amount,
            record.org_loan_fees_charged_amount,
            record.usd_loan_fees_charged_amount,
            record.tzs_loan_fees_charged_amount,
            record.org_loan_fees_paid_amount,
            record.usd_loan_fees_paid_amount,
            record.tzs_loan_fees_paid_amount,
            record.org_tot_monthly_payment_amount,
            record.usd_tot_monthly_payment_amount,
            record.tzs_tot_monthly_payment_amount,
            record.org_interest_paid_total,
            record.usd_interest_paid_total,
            record.tzs_interest_paid_total,
            record.asset_classification_category,
            record.sector_sna_classification,
            record.neg_status_contract,
            record.customer_role,
            record.allowance_probable_loss,
            record.bot_provision
        ))
    
    def get_upsert_query(self) -> str:
        """Get insert query for overdraft"""
        return """
        INSERT INTO overdraft (
            "reportingDate", "accountNumber", "customerIdentificationNumber", "clientName", "clientType",
            "borrowerCountry", "ratingStatus", "crRatingBorrower", "gradesUnratedBanks", "groupCode",
            "relatedEntityName", "relatedParty", "relationshipCategory", "loanProductType", "overdraftEconomicActivity",
            "loanPhase", "transferStatus", "purposeOtherLoans", "contractDate", "branchCode",
            "loanOfficer", "loanSupervisor", currency, "orgSanctionedAmount", "usdSanctionedAmount",
            "tzsSanctionedAmount", "orgUtilisedAmount", "usdUtilisedAmount", "tzsUtilisedAmount",
            "orgCrUsageLast30DaysAmount", "usdCrUsageLast30DaysAmount", "tzsCrUsageLast30DaysAmount",
            "disbursementDate", "expiryDate", "realEndDate", "orgOutstandingAmount", "usdOutstandingAmount",
            "tzsOutstandingAmount", "latestCustomerCreditDate", "latestCreditAmount", "primeLendingRate",
            "annualInterestRate", "collateralPledged", "orgCollateralValue", "usdCollateralValue",
            "tzsCollateralValue", "restructuredLoans", "pastDueDays", "pastDueAmount",
            "orgAccruedInterestAmount", "usdAccruedInterestAmount", "tzsAccruedInterestAmount",
            "orgPenaltyChargedAmount", "usdPenaltyChargedAmount", "tzsPenaltyChargedAmount",
            "orgLoanFeesChargedAmount", "usdLoanFeesChargedAmount", "tzsLoanFeesChargedAmount",
            "orgLoanFeesPaidAmount", "usdLoanFeesPaidAmount", "tzsLoanFeesPaidAmount",
            "orgTotMonthlyPaymentAmount", "usdTotMonthlyPaymentAmount", "tzsTotMonthlyPaymentAmount",
            "orgInterestPaidTotal", "usdInterestPaidTotal", "tzsInterestPaidTotal",
            "assetClassificationCategory", "sectorSnaClassification", "negStatusContract", "customerRole",
            "allowanceProbableLoss", "botProvision"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
    
    def validate_record(self, record: OverdraftRecord) -> bool:
        """Validate Overdraft record"""
        if not super().validate_record(record):
            return False
        
        # Overdraft-specific validations
        if not record.account_number:
            return False
        if not record.client_name:
            return False
        if not record.currency:
            return False
        if record.org_sanctioned_amount is None:
            return False
            
        return True
    
    def transform_data(self, raw_data: Tuple) -> dict:
        """Transform Overdraft data"""
        # Add any Overdraft-specific transformations here
        return {
            'currency_normalized': str(raw_data[22]).strip().upper(),
            'amount_validated': max(0, float(raw_data[23])) if raw_data[23] else 0,
            'account_normalized': str(raw_data[1]).strip()
        }