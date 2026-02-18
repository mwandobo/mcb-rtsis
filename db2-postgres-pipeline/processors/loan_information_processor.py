#!/usr/bin/env python3
"""
Loan Information Record Processor for streaming pipeline
"""

from dataclasses import dataclass
from typing import Optional
from datetime import datetime, date
import logging

@dataclass
class LoanInformationRecord:
    """Data class for loan information records using camelCase"""
    reportingDate: datetime
    customerIdentificationNumber: Optional[str]
    accountNumber: Optional[str]
    clientName: Optional[str]
    borrowerCountry: Optional[str]
    ratingStatus: Optional[str]
    crRatingBorrower: Optional[str]
    gradesUnratedBanks: Optional[str]
    gender: Optional[str]
    disability: Optional[str]
    clientType: Optional[str]
    clientSubType: Optional[str]
    groupName: Optional[str]
    groupCode: Optional[str]
    relatedParty: Optional[str]
    relationshipCategory: Optional[str]
    loanNumber: Optional[str]
    loanType: Optional[str]
    loanEconomicActivity: Optional[str]
    loanPhase: Optional[str]
    transferStatus: Optional[str]
    purposeMortgage: Optional[str]
    purposeOtherLoans: Optional[str]
    sourceFundMortgage: Optional[str]
    amortizationType: Optional[str]
    branchCode: Optional[str]
    loanOfficer: Optional[str]
    loanSupervisor: Optional[str]
    groupVillageNumber: Optional[str]
    cycleNumber: Optional[int]
    loanInstallment: Optional[int]
    repaymentFrequency: Optional[str]
    currency: Optional[str]
    contractDate: Optional[date]
    orgSanctionedAmount: Optional[float]
    usdSanctionedAmount: Optional[float]
    tzsSanctionedAmount: Optional[float]
    orgDisbursedAmount: Optional[float]
    usdDisbursedAmount: Optional[float]
    tzsDisbursedAmount: Optional[float]
    disbursementDate: Optional[date]
    maturityDate: Optional[date]
    realEndDate: Optional[date]
    orgOutstandingPrincipalAmount: Optional[float]
    usdOutstandingPrincipalAmount: Optional[float]
    tzsOutstandingPrincipalAmount: Optional[float]
    orgInstallmentAmount: Optional[float]
    usdInstallmentAmount: Optional[float]
    tzsInstallmentAmount: Optional[float]
    loanInstallmentPaid: Optional[int]
    gracePeriodPaymentPrincipal: Optional[int]
    primeLendingRate: Optional[float]
    interestPricingMethod: Optional[str]
    annualInterestRate: Optional[float]
    effectiveAnnualInterestRate: Optional[float]
    firstInstallmentPaymentDate: Optional[date]
    lastPaymentDate: Optional[date]
    collateralPledged: Optional[str]  # JSON string
    loanFlagType: Optional[str]
    restructuringDate: Optional[date]
    pastDueDays: Optional[int]
    pastDueAmount: Optional[float]
    orgAccruedInterestAmount: Optional[float]
    usdAccruedInterestAmount: Optional[float]
    tzsAccruedInterestAmount: Optional[float]
    orgPenaltyChargedAmount: Optional[float]
    usdPenaltyChargedAmount: Optional[float]
    tzsPenaltyChargedAmount: Optional[float]
    orgPenaltyPaidAmount: Optional[float]
    usdPenaltyPaidAmount: Optional[float]
    tzsPenaltyPaidAmount: Optional[float]
    orgLoanFeesChargedAmount: Optional[float]
    usdLoanFeesChargedAmount: Optional[float]
    tzsLoanFeesChargedAmount: Optional[float]
    orgLoanFeesPaidAmount: Optional[float]
    usdLoanFeesPaidAmount: Optional[float]
    tzsLoanFeesPaidAmount: Optional[float]
    orgTotMonthlyPaymentAmount: Optional[float]
    usdTotMonthlyPaymentAmount: Optional[float]
    tzsTotMonthlyPaymentAmount: Optional[float]
    sectorSnaClassification: Optional[str]
    assetClassificationCategory: Optional[str]
    negStatusContract: Optional[str]
    customerRole: Optional[str]
    allowanceProbableLoss: Optional[float]
    botProvision: Optional[float]
    tradingIntent: Optional[str]
    orgSuspendedInterest: Optional[float]
    usdSuspendedInterest: Optional[float]
    tzsSuspendedInterest: Optional[float]

class LoanInformationProcessor:
    """Processor for loan information records"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def safe_float(self, value):
        """Safely convert value to float, handling various data types"""
        if value is None:
            return None
        
        # If it's already a number
        if isinstance(value, (int, float)):
            return float(value)
        
        # If it's a date/datetime, return None (shouldn't be converted to float)
        if hasattr(value, 'year') or hasattr(value, 'date'):
            return None
            
        # Try to convert string to float
        try:
            str_val = str(value).strip()
            if str_val == '' or str_val.lower() in ['null', 'none', 'n/a']:
                return None
            return float(str_val)
        except (ValueError, TypeError):
            return None
    
    def safe_int(self, value):
        """Safely convert value to int"""
        if value is None:
            return None
            
        # If it's already a number
        if isinstance(value, (int, float)):
            return int(value)
            
        # If it's a date/datetime, return None
        if hasattr(value, 'year') or hasattr(value, 'date'):
            return None
            
        # Try to convert string to int
        try:
            str_val = str(value).strip()
            if str_val == '' or str_val.lower() in ['null', 'none', 'n/a']:
                return None
            return int(float(str_val))  # Convert to float first to handle decimals
        except (ValueError, TypeError):
            return None
    
    def safe_date(self, value):
        """Safely convert value to date, handling various data types"""
        if value is None:
            return None
            
        # If it's already a date/datetime
        if hasattr(value, 'date'):
            return value.date() if hasattr(value, 'date') else value
        elif hasattr(value, 'year'):
            return value
            
        # If it's a string that looks like a currency or other non-date value
        str_val = str(value).strip()
        if str_val == '' or str_val.lower() in ['null', 'none', 'n/a']:
            return None
            
        # If it looks like a currency code or other non-date string, return None
        if len(str_val) <= 5 and str_val.isalpha():
            return None
            
        # Try to parse as date
        try:
            from datetime import datetime
            # Try common date formats
            for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y%m%d']:
                try:
                    return datetime.strptime(str_val, fmt).date()
                except ValueError:
                    continue
            return None
        except:
            return None
    
    def process_record(self, row, record_type='loan_information'):
        """Process a single loan information record from DB2 v2 query (90 fields total: 89 data fields + 1 rn field)"""
        try:
            # Map the fields from the v2 SQL query to the dataclass
            record = LoanInformationRecord(
                reportingDate=row[0] if row[0] else datetime.now(),
                customerIdentificationNumber=str(row[1]).strip() if row[1] else None,
                accountNumber=str(row[2]).strip() if row[2] else None,
                clientName=str(row[3]).strip() if row[3] else None,
                borrowerCountry=str(row[4]).strip() if row[4] else None,
                ratingStatus=str(row[5]).strip() if row[5] else None,
                crRatingBorrower=str(row[6]).strip() if row[6] else None,
                gradesUnratedBanks=str(row[7]).strip() if row[7] else None,
                gender=str(row[8]).strip() if row[8] else None,
                disability=str(row[9]).strip() if row[9] else None,
                clientType=str(row[10]).strip() if row[10] else None,
                clientSubType=str(row[11]).strip() if row[11] else None,
                groupName=str(row[12]).strip() if row[12] else None,
                groupCode=str(row[13]).strip() if row[13] else None,
                relatedParty=str(row[14]).strip() if row[14] else None,
                relationshipCategory=str(row[15]).strip() if row[15] else None,
                loanNumber=str(row[16]).strip() if row[16] else None,
                loanType=str(row[17]).strip() if row[17] else None,
                loanEconomicActivity=str(row[18]).strip() if row[18] else None,
                loanPhase=str(row[19]).strip() if row[19] else None,
                transferStatus=str(row[20]).strip() if row[20] else None,
                purposeMortgage=str(row[21]).strip() if row[21] else None,
                purposeOtherLoans=str(row[22]).strip() if row[22] else None,
                sourceFundMortgage=str(row[23]).strip() if row[23] else None,
                amortizationType=str(row[24]).strip() if row[24] else None,
                branchCode=str(row[25]).strip() if row[25] else None,
                loanOfficer=str(row[26]).strip() if row[26] else None,
                loanSupervisor=str(row[27]).strip() if row[27] else None,
                groupVillageNumber=str(row[28]).strip() if row[28] else None,
                cycleNumber=self.safe_int(row[29]),
                loanInstallment=self.safe_int(row[30]),
                repaymentFrequency=str(row[31]).strip() if row[31] else None,
                currency=str(row[32]).strip() if row[32] else None,
                contractDate=self.safe_date(row[33]),
                orgSanctionedAmount=self.safe_float(row[34]),
                usdSanctionedAmount=self.safe_float(row[35]),
                tzsSanctionedAmount=self.safe_float(row[36]),
                orgDisbursedAmount=self.safe_float(row[37]),
                usdDisbursedAmount=self.safe_float(row[38]),
                tzsDisbursedAmount=self.safe_float(row[39]),
                disbursementDate=self.safe_date(row[40]),
                maturityDate=self.safe_date(row[41]),
                realEndDate=self.safe_date(row[42]),
                orgOutstandingPrincipalAmount=self.safe_float(row[43]),
                usdOutstandingPrincipalAmount=self.safe_float(row[44]),
                tzsOutstandingPrincipalAmount=self.safe_float(row[45]),
                orgInstallmentAmount=self.safe_float(row[46]),
                usdInstallmentAmount=self.safe_float(row[47]),
                tzsInstallmentAmount=self.safe_float(row[48]),
                loanInstallmentPaid=self.safe_int(row[49]),
                gracePeriodPaymentPrincipal=self.safe_int(row[50]),
                primeLendingRate=self.safe_float(row[51]),
                interestPricingMethod=str(row[52]).strip() if row[52] else None,
                annualInterestRate=self.safe_float(row[53]),
                effectiveAnnualInterestRate=self.safe_float(row[54]),
                firstInstallmentPaymentDate=self.safe_date(row[55]),
                lastPaymentDate=self.safe_date(row[56]),
                collateralPledged=str(row[57]).strip() if row[57] else None,  # JSON string
                loanFlagType=str(row[58]).strip() if row[58] else None,
                restructuringDate=self.safe_date(row[59]),
                pastDueDays=self.safe_int(row[60]),
                pastDueAmount=self.safe_float(row[61]),
                orgAccruedInterestAmount=self.safe_float(row[62]),
                usdAccruedInterestAmount=self.safe_float(row[63]),
                tzsAccruedInterestAmount=self.safe_float(row[64]),
                orgPenaltyChargedAmount=self.safe_float(row[65]),
                usdPenaltyChargedAmount=self.safe_float(row[66]),
                tzsPenaltyChargedAmount=self.safe_float(row[67]),
                orgPenaltyPaidAmount=self.safe_float(row[68]),
                usdPenaltyPaidAmount=self.safe_float(row[69]),
                tzsPenaltyPaidAmount=self.safe_float(row[70]),
                orgLoanFeesChargedAmount=self.safe_float(row[71]),
                usdLoanFeesChargedAmount=self.safe_float(row[72]),
                tzsLoanFeesChargedAmount=self.safe_float(row[73]),
                orgLoanFeesPaidAmount=self.safe_float(row[74]),
                usdLoanFeesPaidAmount=self.safe_float(row[75]),
                tzsLoanFeesPaidAmount=self.safe_float(row[76]),
                orgTotMonthlyPaymentAmount=self.safe_float(row[77]),
                usdTotMonthlyPaymentAmount=self.safe_float(row[78]),
                tzsTotMonthlyPaymentAmount=self.safe_float(row[79]),
                sectorSnaClassification=str(row[80]).strip() if row[80] else None,
                assetClassificationCategory=str(row[81]).strip() if row[81] else None,
                negStatusContract=str(row[82]).strip() if row[82] else None,
                customerRole=str(row[83]).strip() if row[83] else None,
                allowanceProbableLoss=self.safe_float(row[84]),
                botProvision=self.safe_float(row[85]),
                tradingIntent=str(row[86]).strip() if row[86] else None,
                orgSuspendedInterest=self.safe_float(row[87]),
                usdSuspendedInterest=self.safe_float(row[88]),
                tzsSuspendedInterest=self.safe_float(row[89])
                # Note: row[90] (rn) is the last field - ROW_NUMBER field
            )
            
            return record
            
        except Exception as e:
            self.logger.error(f"Error processing loan information record: {e}")
            self.logger.error(f"Row data length: {len(row) if row else 'None'}")
            if row:
                self.logger.error(f"Row data: {row[:5]}...")  # Show first 5 fields
            raise
    
    def validate_record(self, record):
        """Validate loan information record"""
        try:
            # Basic validation
            if not record.reportingDate:
                self.logger.warning("Missing reporting date")
                return False
            
            if not record.accountNumber:
                self.logger.warning("Missing account number")
                return False
            
            if record.orgSanctionedAmount is not None and record.orgSanctionedAmount < 0:
                self.logger.warning("Negative sanctioned amount")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating loan information record: {e}")
            return False
    
    def insert_to_postgres(self, record, cursor):
        """Insert loan information record to PostgreSQL using camelCase columns - INSERT ALL records"""
        try:
            insert_query = """
            INSERT INTO "loanInformation" (
                "reportingDate", "customerIdentificationNumber", "accountNumber", "clientName",
                "borrowerCountry", "ratingStatus", "crRatingBorrower", "gradesUnratedBanks",
                "gender", "disability", "clientType", "clientSubType", "groupName", "groupCode",
                "relatedParty", "relationshipCategory", "loanNumber", "loanType", "loanEconomicActivity",
                "loanPhase", "transferStatus", "purposeMortgage", "purposeOtherLoans", "sourceFundMortgage",
                "amortizationType", "branchCode", "loanOfficer", "loanSupervisor", "groupVillageNumber",
                "cycleNumber", "loanInstallment", "repaymentFrequency", "currency", "contractDate",
                "orgSanctionedAmount", "usdSanctionedAmount", "tzsSanctionedAmount", "orgDisbursedAmount",
                "usdDisbursedAmount", "tzsDisbursedAmount", "disbursementDate", "maturityDate", "realEndDate",
                "orgOutstandingPrincipalAmount", "usdOutstandingPrincipalAmount", "tzsOutstandingPrincipalAmount",
                "orgInstallmentAmount", "usdInstallmentAmount", "tzsInstallmentAmount", "loanInstallmentPaid",
                "gracePeriodPaymentPrincipal", "primeLendingRate", "interestPricingMethod", "annualInterestRate",
                "effectiveAnnualInterestRate", "firstInstallmentPaymentDate", "lastPaymentDate", "collateralPledged",
                "loanFlagType", "restructuringDate", "pastDueDays", "pastDueAmount",
                "orgAccruedInterestAmount", "usdAccruedInterestAmount", "tzsAccruedInterestAmount",
                "orgPenaltyChargedAmount", "usdPenaltyChargedAmount", "tzsPenaltyChargedAmount", "orgPenaltyPaidAmount",
                "usdPenaltyPaidAmount", "tzsPenaltyPaidAmount", "orgLoanFeesChargedAmount", "usdLoanFeesChargedAmount",
                "tzsLoanFeesChargedAmount", "orgLoanFeesPaidAmount", "usdLoanFeesPaidAmount", "tzsLoanFeesPaidAmount",
                "orgTotMonthlyPaymentAmount", "usdTotMonthlyPaymentAmount", "tzsTotMonthlyPaymentAmount",
                "sectorSnaClassification", "assetClassificationCategory", "negStatusContract", "customerRole",
                "allowanceProbableLoss", "botProvision", "tradingIntent", "orgSuspendedInterest",
                "usdSuspendedInterest", "tzsSuspendedInterest"
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            
            cursor.execute(insert_query, (
                record.reportingDate, record.customerIdentificationNumber, record.accountNumber, record.clientName,
                record.borrowerCountry, record.ratingStatus, record.crRatingBorrower, record.gradesUnratedBanks,
                record.gender, record.disability, record.clientType, record.clientSubType, record.groupName, record.groupCode,
                record.relatedParty, record.relationshipCategory, record.loanNumber, record.loanType, record.loanEconomicActivity,
                record.loanPhase, record.transferStatus, record.purposeMortgage, record.purposeOtherLoans, record.sourceFundMortgage,
                record.amortizationType, record.branchCode, record.loanOfficer, record.loanSupervisor, record.groupVillageNumber,
                record.cycleNumber, record.loanInstallment, record.repaymentFrequency, record.currency, record.contractDate,
                record.orgSanctionedAmount, record.usdSanctionedAmount, record.tzsSanctionedAmount, record.orgDisbursedAmount,
                record.usdDisbursedAmount, record.tzsDisbursedAmount, record.disbursementDate, record.maturityDate, record.realEndDate,
                record.orgOutstandingPrincipalAmount, record.usdOutstandingPrincipalAmount, record.tzsOutstandingPrincipalAmount,
                record.orgInstallmentAmount, record.usdInstallmentAmount, record.tzsInstallmentAmount, record.loanInstallmentPaid,
                record.gracePeriodPaymentPrincipal, record.primeLendingRate, record.interestPricingMethod, record.annualInterestRate,
                record.effectiveAnnualInterestRate, record.firstInstallmentPaymentDate, record.lastPaymentDate, record.collateralPledged,
                record.loanFlagType, record.restructuringDate, record.pastDueDays, record.pastDueAmount,
                record.orgAccruedInterestAmount, record.usdAccruedInterestAmount, record.tzsAccruedInterestAmount,
                record.orgPenaltyChargedAmount, record.usdPenaltyChargedAmount, record.tzsPenaltyChargedAmount, record.orgPenaltyPaidAmount,
                record.usdPenaltyPaidAmount, record.tzsPenaltyPaidAmount, record.orgLoanFeesChargedAmount, record.usdLoanFeesChargedAmount,
                record.tzsLoanFeesChargedAmount, record.orgLoanFeesPaidAmount, record.usdLoanFeesPaidAmount, record.tzsLoanFeesPaidAmount,
                record.orgTotMonthlyPaymentAmount, record.usdTotMonthlyPaymentAmount, record.tzsTotMonthlyPaymentAmount,
                record.sectorSnaClassification, record.assetClassificationCategory, record.negStatusContract, record.customerRole,
                record.allowanceProbableLoss, record.botProvision, record.tradingIntent, record.orgSuspendedInterest,
                record.usdSuspendedInterest, record.tzsSuspendedInterest
            ))
            
            # Return INSERT since we're always inserting
            return "INSERT"
            
        except Exception as e:
            self.logger.error(f"Error inserting loan information record to PostgreSQL: {e}")
            raise