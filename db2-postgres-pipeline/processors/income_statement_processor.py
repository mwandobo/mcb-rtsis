"""
Income statement processor - Based on income-statement.sql
Handles conversion of comma-separated itemCode:value strings to JSONB arrays
"""

from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime
import json
from .base import BaseProcessor, BaseRecord

@dataclass
class IncomeStatementRecord(BaseRecord):
    """Income statement record structure"""
    reporting_date: str
    interest_income: str  # Comma-separated "itemCode:value" pairs
    interest_income_value: Optional[float]
    interest_expenses: str
    interest_expenses_value: Optional[float]
    bad_debts_written_off_not_provided: Optional[float]
    provision_bad_doubtful_debts: Optional[float]
    impairments_investments: Optional[float]
    income_tax_provision: Optional[float]
    extraordinary_credits_charge: Optional[float]
    non_core_credits_charges: str
    non_core_credits_charges_value: Optional[float]
    non_interest_income: str
    non_interest_income_value: Optional[float]
    non_interest_expenses: str
    non_interest_expenses_value: Optional[float]
    retry_count: int = 0

class IncomeStatementProcessor(BaseProcessor):
    """Processor for income statement data"""
    
    def parse_item_codes(self, csv_string: str) -> list:
        """
        Convert comma-separated "itemCode:value" string to JSON array
        Input: "1:12345.67,2:98765.43,6:54321.00"
        Output: [{"itemCode": 1, "value": 12345.67}, {"itemCode": 2, "value": 98765.43}, ...]
        """
        if not csv_string or csv_string.strip() == '':
            return []
        
        result = []
        pairs = csv_string.strip().split(',')
        
        for pair in pairs:
            if ':' in pair:
                try:
                    code, value = pair.split(':', 1)
                    result.append({
                        "itemCode": int(code.strip()),
                        "value": float(value.strip())
                    })
                except (ValueError, AttributeError):
                    continue
        
        return result
    
    def process_record(self, raw_data: Tuple, table_name: str) -> IncomeStatementRecord:
        """Convert raw DB2 data to IncomeStatementRecord"""
        # Handle None values safely
        def safe_str(value):
            return str(value).strip() if value is not None else ''
        
        def safe_float(value):
            try:
                return float(value) if value is not None else 0.0
            except (ValueError, TypeError):
                return 0.0
        
        return IncomeStatementRecord(
            source_table=table_name,
            timestamp_column_value=safe_str(raw_data[0]),  # reportingDate for tracking
            reporting_date=safe_str(raw_data[0]),
            interest_income=safe_str(raw_data[1]),
            interest_income_value=safe_float(raw_data[2]),
            interest_expenses=safe_str(raw_data[3]),
            interest_expenses_value=safe_float(raw_data[4]),
            bad_debts_written_off_not_provided=safe_float(raw_data[5]),
            provision_bad_doubtful_debts=safe_float(raw_data[6]),
            impairments_investments=safe_float(raw_data[7]),
            income_tax_provision=safe_float(raw_data[8]),
            extraordinary_credits_charge=safe_float(raw_data[9]),
            non_core_credits_charges=safe_str(raw_data[10]),
            non_core_credits_charges_value=safe_float(raw_data[11]),
            non_interest_income=safe_str(raw_data[12]),
            non_interest_income_value=safe_float(raw_data[13]),
            non_interest_expenses=safe_str(raw_data[14]),
            non_interest_expenses_value=safe_float(raw_data[15]),
            original_timestamp=datetime.now().isoformat()
        )
    
    def insert_to_postgres(self, record: IncomeStatementRecord, pg_cursor) -> None:
        """Insert income statement record to PostgreSQL with JSONB conversion"""
        query = self.get_insert_query()
        
        # Convert comma-separated strings to JSON arrays
        interest_income_json = json.dumps(self.parse_item_codes(record.interest_income))
        interest_expenses_json = json.dumps(self.parse_item_codes(record.interest_expenses))
        non_core_credits_json = json.dumps(self.parse_item_codes(record.non_core_credits_charges))
        non_interest_income_json = json.dumps(self.parse_item_codes(record.non_interest_income))
        non_interest_expenses_json = json.dumps(self.parse_item_codes(record.non_interest_expenses))
        
        pg_cursor.execute(query, (
            record.reporting_date,
            interest_income_json,
            record.interest_income_value,
            interest_expenses_json,
            record.interest_expenses_value,
            record.bad_debts_written_off_not_provided,
            record.provision_bad_doubtful_debts,
            record.impairments_investments,
            record.income_tax_provision,
            record.extraordinary_credits_charge,
            non_core_credits_json,
            record.non_core_credits_charges_value,
            non_interest_income_json,
            record.non_interest_income_value,
            non_interest_expenses_json,
            record.non_interest_expenses_value
        ))
    
    def upsert_to_postgres(self, record: IncomeStatementRecord, pg_cursor) -> None:
        """Upsert income statement record to PostgreSQL with JSONB conversion"""
        query = self.get_upsert_query()
        
        # Convert comma-separated strings to JSON arrays
        interest_income_json = json.dumps(self.parse_item_codes(record.interest_income))
        interest_expenses_json = json.dumps(self.parse_item_codes(record.interest_expenses))
        non_core_credits_json = json.dumps(self.parse_item_codes(record.non_core_credits_charges))
        non_interest_income_json = json.dumps(self.parse_item_codes(record.non_interest_income))
        non_interest_expenses_json = json.dumps(self.parse_item_codes(record.non_interest_expenses))
        
        pg_cursor.execute(query, (
            record.reporting_date,
            interest_income_json,
            record.interest_income_value,
            interest_expenses_json,
            record.interest_expenses_value,
            record.bad_debts_written_off_not_provided,
            record.provision_bad_doubtful_debts,
            record.impairments_investments,
            record.income_tax_provision,
            record.extraordinary_credits_charge,
            non_core_credits_json,
            record.non_core_credits_charges_value,
            non_interest_income_json,
            record.non_interest_income_value,
            non_interest_expenses_json,
            record.non_interest_expenses_value
        ))
    
    def get_insert_query(self) -> str:
        """Get insert query for income statement"""
        return """
        INSERT INTO "incomeStatement" (
            "reportingDate", "interestIncome", "interestIncomeValue", "interestExpenses", "interestExpensesValue",
            "badDebtsWrittenOffNotProvided", "provisionBadDoubtfulDebts", "impairmentsInvestments",
            "incomeTaxProvision", "extraordinaryCreditsCharge", "nonCoreCreditsCharges", "nonCoreCreditsChargesValue",
            "nonInterestIncome", "nonInterestIncomeValue", "nonInterestExpenses", "nonInterestExpensesValue"
        ) VALUES (
            %s, %s::jsonb, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s::jsonb, %s, %s::jsonb, %s
        )
        """
    
    def get_upsert_query(self) -> str:
        """Get upsert query for income statement (replace existing by reporting date)"""
        return """
        INSERT INTO "incomeStatement" (
            "reportingDate", "interestIncome", "interestIncomeValue", "interestExpenses", "interestExpensesValue",
            "badDebtsWrittenOffNotProvided", "provisionBadDoubtfulDebts", "impairmentsInvestments",
            "incomeTaxProvision", "extraordinaryCreditsCharge", "nonCoreCreditsCharges", "nonCoreCreditsChargesValue",
            "nonInterestIncome", "nonInterestIncomeValue", "nonInterestExpenses", "nonInterestExpensesValue"
        ) VALUES (
            %s, %s::jsonb, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s::jsonb, %s, %s::jsonb, %s
        )
        ON CONFLICT ("reportingDate") DO UPDATE SET
            "interestIncome" = EXCLUDED."interestIncome",
            "interestIncomeValue" = EXCLUDED."interestIncomeValue",
            "interestExpenses" = EXCLUDED."interestExpenses",
            "interestExpensesValue" = EXCLUDED."interestExpensesValue",
            "badDebtsWrittenOffNotProvided" = EXCLUDED."badDebtsWrittenOffNotProvided",
            "provisionBadDoubtfulDebts" = EXCLUDED."provisionBadDoubtfulDebts",
            "impairmentsInvestments" = EXCLUDED."impairmentsInvestments",
            "incomeTaxProvision" = EXCLUDED."incomeTaxProvision",
            "extraordinaryCreditsCharge" = EXCLUDED."extraordinaryCreditsCharge",
            "nonCoreCreditsCharges" = EXCLUDED."nonCoreCreditsCharges",
            "nonCoreCreditsChargesValue" = EXCLUDED."nonCoreCreditsChargesValue",
            "nonInterestIncome" = EXCLUDED."nonInterestIncome",
            "nonInterestIncomeValue" = EXCLUDED."nonInterestIncomeValue",
            "nonInterestExpenses" = EXCLUDED."nonInterestExpenses",
            "nonInterestExpensesValue" = EXCLUDED."nonInterestExpensesValue"
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
            'net_interest_income': (raw_data[2] or 0) - (raw_data[4] or 0),
            'total_income': (raw_data[2] or 0) + (raw_data[13] or 0),
            'total_expenses': (raw_data[4] or 0) + (raw_data[15] or 0)
        }
