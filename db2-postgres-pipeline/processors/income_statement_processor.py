"""
Income statement processor - Based on income-statement.sql
"""

from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime
from .base import BaseProcessor, BaseRecord

@dataclass
class IncomeStatementRecord(BaseRecord):
    """Income statement record structure"""
    reporting_date: str
    interest_income: Optional[float]
    interest_expense: Optional[float]
    bad_debts_written_off_not_provided: Optional[float]
    provision_bad_doubtful_debts: Optional[float]
    impairments_investments: Optional[float]
    non_interest_income: Optional[float]
    non_interest_expenses: Optional[float]
    income_tax_provision: Optional[float]
    extraordinary_credits_charge: Optional[float]
    non_core_credits_charges: Optional[float]
    amount_interest_income: Optional[float]
    amount_interest_expenses: Optional[float]
    amount_non_interest_income: Optional[float]
    amount_non_interest_expenses: Optional[float]
    amount_non_core_credits_charges: Optional[float]
    retry_count: int = 0

class IncomeStatementProcessor(BaseProcessor):
    """Processor for income statement data"""
    
    def process_record(self, raw_data: Tuple, table_name: str) -> IncomeStatementRecord:
        """Convert raw DB2 data to IncomeStatementRecord"""
        # Handle None values safely
        def safe_str(value):
            return str(value).strip() if value is not None else None
        
        def safe_float(value):
            try:
                return float(value) if value is not None else 0.0
            except (ValueError, TypeError):
                return 0.0
        
        return IncomeStatementRecord(
            source_table=table_name,
            timestamp_column_value=safe_str(raw_data[0]),  # reportingDate for tracking
            reporting_date=safe_str(raw_data[0]),
            interest_income=safe_float(raw_data[1]),
            interest_expense=safe_float(raw_data[2]),
            bad_debts_written_off_not_provided=safe_float(raw_data[3]),
            provision_bad_doubtful_debts=safe_float(raw_data[4]),
            impairments_investments=safe_float(raw_data[5]),
            non_interest_income=safe_float(raw_data[6]),
            non_interest_expenses=safe_float(raw_data[7]),
            income_tax_provision=safe_float(raw_data[8]),
            extraordinary_credits_charge=safe_float(raw_data[9]),
            non_core_credits_charges=safe_float(raw_data[10]),
            amount_interest_income=safe_float(raw_data[11]),
            amount_interest_expenses=safe_float(raw_data[12]),
            amount_non_interest_income=safe_float(raw_data[13]),
            amount_non_interest_expenses=safe_float(raw_data[14]),
            amount_non_core_credits_charges=safe_float(raw_data[15]),
            original_timestamp=datetime.now().isoformat()
        )
    
    def insert_to_postgres(self, record: IncomeStatementRecord, pg_cursor) -> None:
        """Insert income statement record to PostgreSQL"""
        query = self.get_insert_query()
        
        pg_cursor.execute(query, (
            record.reporting_date,
            record.interest_income,
            record.interest_expense,
            record.bad_debts_written_off_not_provided,
            record.provision_bad_doubtful_debts,
            record.impairments_investments,
            record.non_interest_income,
            record.non_interest_expenses,
            record.income_tax_provision,
            record.extraordinary_credits_charge,
            record.non_core_credits_charges,
            record.amount_interest_income,
            record.amount_interest_expenses,
            record.amount_non_interest_income,
            record.amount_non_interest_expenses,
            record.amount_non_core_credits_charges
        ))
    
    def upsert_to_postgres(self, record: IncomeStatementRecord, pg_cursor) -> None:
        """Upsert income statement record to PostgreSQL"""
        query = self.get_insert_query()  # Use simple insert since we don't have unique constraint
        
        pg_cursor.execute(query, (
            record.reporting_date,
            record.interest_income,
            record.interest_expense,
            record.bad_debts_written_off_not_provided,
            record.provision_bad_doubtful_debts,
            record.impairments_investments,
            record.non_interest_income,
            record.non_interest_expenses,
            record.income_tax_provision,
            record.extraordinary_credits_charge,
            record.non_core_credits_charges,
            record.amount_interest_income,
            record.amount_interest_expenses,
            record.amount_non_interest_income,
            record.amount_non_interest_expenses,
            record.amount_non_core_credits_charges
        ))
    
    def get_insert_query(self) -> str:
        """Get insert query for income statement"""
        return """
        INSERT INTO "incomeStatement" (
            "reportingDate", "interestIncome", "interestExpense", "badDebtsWrittenOffNotProvided",
            "provisionBadDoubtfulDebts", "impairmentsInvestments", "nonInterestIncome", "nonInterestExpenses",
            "incomeTaxProvision", "extraordinaryCreditsCharge", "nonCoreCreditsCharges",
            "amountInterestIncome", "amountInterestExpenses", "amountNonInterestIncome",
            "amountNonInterestExpenses", "amountnonCoreCreditsCharges"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
    
    def get_upsert_query(self) -> str:
        """Get upsert query for income statement (replace existing by reporting date)"""
        return """
        INSERT INTO "incomeStatement" (
            "reportingDate", "interestIncome", "interestExpense", "badDebtsWrittenOffNotProvided",
            "provisionBadDoubtfulDebts", "impairmentsInvestments", "nonInterestIncome", "nonInterestExpenses",
            "incomeTaxProvision", "extraordinaryCreditsCharge", "nonCoreCreditsCharges",
            "amountInterestIncome", "amountInterestExpenses", "amountNonInterestIncome",
            "amountNonInterestExpenses", "amountnonCoreCreditsCharges"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT ("reportingDate") DO UPDATE SET
            "interestIncome" = EXCLUDED."interestIncome",
            "interestExpense" = EXCLUDED."interestExpense",
            "badDebtsWrittenOffNotProvided" = EXCLUDED."badDebtsWrittenOffNotProvided",
            "provisionBadDoubtfulDebts" = EXCLUDED."provisionBadDoubtfulDebts",
            "impairmentsInvestments" = EXCLUDED."impairmentsInvestments",
            "nonInterestIncome" = EXCLUDED."nonInterestIncome",
            "nonInterestExpenses" = EXCLUDED."nonInterestExpenses",
            "incomeTaxProvision" = EXCLUDED."incomeTaxProvision",
            "extraordinaryCreditsCharge" = EXCLUDED."extraordinaryCreditsCharge",
            "nonCoreCreditsCharges" = EXCLUDED."nonCoreCreditsCharges",
            "amountInterestIncome" = EXCLUDED."amountInterestIncome",
            "amountInterestExpenses" = EXCLUDED."amountInterestExpenses",
            "amountNonInterestIncome" = EXCLUDED."amountNonInterestIncome",
            "amountNonInterestExpenses" = EXCLUDED."amountNonInterestExpenses",
            "amountnonCoreCreditsCharges" = EXCLUDED."amountnonCoreCreditsCharges"
        """
    
    def validate_record(self, record: IncomeStatementRecord) -> bool:
        """Validate income statement record"""
        if not super().validate_record(record):
            return False
        
        # Income statement-specific validations
        if not record.reporting_date:
            return False
            
        return True
    
    def transform_data(self, raw_data: Tuple) -> dict:
        """Transform income statement data"""
        # Add any income statement-specific transformations here
        return {
            'net_interest_income': (raw_data[1] or 0) - (raw_data[2] or 0),
            'total_income': (raw_data[1] or 0) + (raw_data[6] or 0),
            'total_expenses': (raw_data[2] or 0) + (raw_data[7] or 0)
        }