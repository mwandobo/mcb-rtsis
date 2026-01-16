"""
Inter-bank loan receivable processor V2 - Based on inter-bank-loan-receivable-v2.sql
"""

from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime
from .base import BaseProcessor, BaseRecord

@dataclass
class InterBankLoanReceivableRecord(BaseRecord):
    """Inter-bank loan receivable record structure - V2"""
    reporting_date: str
    borrowers_institution_code: Optional[str]
    borrower_country: Optional[str]
    relationship_type: Optional[str]
    rating_status: Optional[int]
    external_rating_correspondent_borrower: Optional[str]
    grades_unrated_borrower: Optional[str]
    loan_number: Optional[str]
    loan_type: Optional[str]
    issue_date: Optional[str]
    loan_maturity_date: Optional[str]
    currency: Optional[str]
    org_loan_amount: Optional[float]
    usd_loan_amount: Optional[float]
    tzs_loan_amount: Optional[float]
    interest_rate: Optional[float]
    org_accrued_interest_amount: Optional[float]
    usd_accrued_interest_amount: Optional[float]
    tzs_accrued_interest_amount: Optional[float]
    org_suspended_interest: Optional[float]
    usd_suspended_interest: Optional[float]
    tzs_suspended_interest: Optional[float]
    past_due_days: Optional[int]
    allowance_probable_loss: Optional[float]
    bot_provision: Optional[float]
    asset_classification_category: Optional[str]
    retry_count: int = 0

class InterBankLoanReceivableProcessor(BaseProcessor):
    """Processor for inter-bank loan receivable data - V2"""
    
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
            timestamp_column_value=safe_str(raw_data[9]),  # issueDate for tracking
            reporting_date=safe_str(raw_data[0]),
            borrowers_institution_code=safe_str(raw_data[1]),
            borrower_country=safe_str(raw_data[2]),
            relationship_type=safe_str(raw_data[3]),
            rating_status=safe_int(raw_data[4]),
            external_rating_correspondent_borrower=safe_str(raw_data[5]),
            grades_unrated_borrower=safe_str(raw_data[6]),
            loan_number=safe_str(raw_data[7]),
            loan_type=safe_str(raw_data[8]),
            issue_date=safe_str(raw_data[9]),
            loan_maturity_date=safe_str(raw_data[10]),
            currency=safe_str(raw_data[11]),
            org_loan_amount=safe_float(raw_data[12]),
            usd_loan_amount=safe_float(raw_data[13]),
            tzs_loan_amount=safe_float(raw_data[14]),
            interest_rate=safe_float(raw_data[15]),
            org_accrued_interest_amount=safe_float(raw_data[16]),
            usd_accrued_interest_amount=safe_float(raw_data[17]),
            tzs_accrued_interest_amount=safe_float(raw_data[18]),
            org_suspended_interest=safe_float(raw_data[19]),
            usd_suspended_interest=safe_float(raw_data[20]),
            tzs_suspended_interest=safe_float(raw_data[21]),
            past_due_days=safe_int(raw_data[22]),
            allowance_probable_loss=safe_float(raw_data[23]),
            bot_provision=safe_float(raw_data[24]),
            asset_classification_category=safe_str(raw_data[25]),
            original_timestamp=datetime.now().isoformat()
        )
    
    def insert_to_postgres(self, record: InterBankLoanReceivableRecord, pg_cursor) -> None:
        """Insert inter-bank loan receivable record to PostgreSQL"""
        query = self.get_insert_query()
        
        pg_cursor.execute(query, (
            record.reporting_date,
            record.borrowers_institution_code,
            record.borrower_country,
            record.relationship_type,
            record.rating_status,
            record.external_rating_correspondent_borrower,
            record.grades_unrated_borrower,
            record.loan_number,
            record.loan_type,
            record.issue_date,
            record.loan_maturity_date,
            record.currency,
            record.org_loan_amount,
            record.usd_loan_amount,
            record.tzs_loan_amount,
            record.interest_rate,
            record.org_accrued_interest_amount,
            record.usd_accrued_interest_amount,
            record.tzs_accrued_interest_amount,
            record.org_suspended_interest,
            record.usd_suspended_interest,
            record.tzs_suspended_interest,
            record.past_due_days,
            record.allowance_probable_loss,
            record.bot_provision,
            record.asset_classification_category
        ))
    
    def get_insert_query(self) -> str:
        """Get insert query for inter-bank loan receivable"""
        return """
        INSERT INTO "interBankLoanReceivable" (
            "reportingDate", "borrowersInstitutionCode", "borrowerCountry", "relationshipType",
            "ratingStatus", "externalRatingCorrespondentBorrower", "gradesUnratedBorrower",
            "loanNumber", "loanType", "issueDate", "loanMaturityDate", "currency",
            "orgLoanAmount", "usdLoanAmount", "tzsLoanAmount", "interestRate",
            "orgAccruedInterestAmount", "usdAccruedInterestAmount", "tzsAccruedInterestAmount",
            "orgSuspendedInterest", "usdSuspendedInterest", "tzsSuspendedInterest",
            "pastDueDays", "allowanceProbableLoss", "botProvision", "assetClassificationCategory"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s
        )
        """
    
    def get_upsert_query(self) -> str:
        """Get upsert query for inter-bank loan receivable (use loan number as unique key)"""
        return """
        INSERT INTO "interBankLoanReceivable" (
            "reportingDate", "borrowersInstitutionCode", "borrowerCountry", "relationshipType",
            "ratingStatus", "externalRatingCorrespondentBorrower", "gradesUnratedBorrower",
            "loanNumber", "loanType", "issueDate", "loanMaturityDate", "currency",
            "orgLoanAmount", "usdLoanAmount", "tzsLoanAmount", "interestRate",
            "orgAccruedInterestAmount", "usdAccruedInterestAmount", "tzsAccruedInterestAmount",
            "orgSuspendedInterest", "usdSuspendedInterest", "tzsSuspendedInterest",
            "pastDueDays", "allowanceProbableLoss", "botProvision", "assetClassificationCategory"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT ("loanNumber") DO UPDATE SET
            "reportingDate" = EXCLUDED."reportingDate",
            "borrowersInstitutionCode" = EXCLUDED."borrowersInstitutionCode",
            "borrowerCountry" = EXCLUDED."borrowerCountry",
            "relationshipType" = EXCLUDED."relationshipType",
            "ratingStatus" = EXCLUDED."ratingStatus",
            "externalRatingCorrespondentBorrower" = EXCLUDED."externalRatingCorrespondentBorrower",
            "gradesUnratedBorrower" = EXCLUDED."gradesUnratedBorrower",
            "loanType" = EXCLUDED."loanType",
            "issueDate" = EXCLUDED."issueDate",
            "loanMaturityDate" = EXCLUDED."loanMaturityDate",
            "currency" = EXCLUDED."currency",
            "orgLoanAmount" = EXCLUDED."orgLoanAmount",
            "usdLoanAmount" = EXCLUDED."usdLoanAmount",
            "tzsLoanAmount" = EXCLUDED."tzsLoanAmount",
            "interestRate" = EXCLUDED."interestRate",
            "orgAccruedInterestAmount" = EXCLUDED."orgAccruedInterestAmount",
            "usdAccruedInterestAmount" = EXCLUDED."usdAccruedInterestAmount",
            "tzsAccruedInterestAmount" = EXCLUDED."tzsAccruedInterestAmount",
            "orgSuspendedInterest" = EXCLUDED."orgSuspendedInterest",
            "usdSuspendedInterest" = EXCLUDED."usdSuspendedInterest",
            "tzsSuspendedInterest" = EXCLUDED."tzsSuspendedInterest",
            "pastDueDays" = EXCLUDED."pastDueDays",
            "allowanceProbableLoss" = EXCLUDED."allowanceProbableLoss",
            "botProvision" = EXCLUDED."botProvision",
            "assetClassificationCategory" = EXCLUDED."assetClassificationCategory"
        """
    
    def validate_record(self, record: InterBankLoanReceivableRecord) -> bool:
        """Validate inter-bank loan receivable record"""
        if not super().validate_record(record):
            return False
        
        # Inter-bank loan receivable-specific validations
        # Only require institution code - loan number can be empty
        if not record.borrowers_institution_code:
            return False
        if not record.currency:
            return False
            
        return True
    
    def transform_data(self, raw_data: Tuple) -> dict:
        """Transform inter-bank loan receivable data"""
        # Add any inter-bank loan receivable-specific transformations here
        return {
            'institution_code_normalized': str(raw_data[1]).strip() if raw_data[1] else '',
            'currency_normalized': str(raw_data[11]).strip().upper() if raw_data[11] else 'TZS',
            'loan_type_normalized': str(raw_data[8]).strip().title() if raw_data[8] else 'Unknown'
        }
