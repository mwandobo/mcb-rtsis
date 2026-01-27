#!/usr/bin/env python3
"""
Overdraft Record Processor for streaming pipeline
"""

from dataclasses import dataclass
from typing import Optional
from datetime import datetime
import logging

@dataclass
class OverdraftRecord:
    """Data class for overdraft records"""
    reportingDate: str
    custId: str
    accountNumber: str
    customerIdentificationNumber: Optional[str]
    clientName: str
    clientType: Optional[str]
    borrowerCountry: Optional[str]
    ratingStatus: int
    crRatingBorrower: Optional[str]
    gradesUnratedBanks: Optional[str]
    groupCode: Optional[str]
    relatedEntityName: Optional[str]
    relatedParty: Optional[str]
    relationshipCategory: Optional[str]
    loanProductType: Optional[str]
    idProduct: Optional[str]
    overdraftEconomicActivity: str
    loanPhase: str
    transferStatus: str
    purposeOtherLoans: str
    contractDate: Optional[datetime]
    branchCode: Optional[str]
    loanOfficer: Optional[str]
    loanSupervisor: Optional[str]
    currency: str
    orgSanctionedAmount: Optional[float]
    usdSanctionedAmount: Optional[float]
    tzsSanctionedAmount: Optional[float]
    orgUtilisedAmount: Optional[float]
    usdUtilisedAmount: Optional[float]
    tzsUtilisedAmount: Optional[float]
    orgCrUsageLast30DaysAmount: Optional[float]
    usdCrUsageLast30DaysAmount: Optional[float]
    tzsCrUsageLast30DaysAmount: Optional[float]
    disbursementDate: Optional[datetime]
    expiryDate: Optional[datetime]
    realEndDate: Optional[datetime]
    orgOutstandingAmount: Optional[float]
    usdOutstandingAmount: Optional[float]
    tzsOutstandingAmount: Optional[float]
    orgOutstandingPrincipalAmount: Optional[float]
    usdOutstandingPrincipalAmount: Optional[float]
    tzsOutstandingPrincipalAmount: Optional[float]
    latestCustomerCreditDate: Optional[datetime]
    latestCreditAmount: Optional[float]
    primeLendingRate: Optional[float]
    annualInterestRate: Optional[float]
    collateralPledged: Optional[str]
    orgCollateralValue: Optional[float]
    usdCollateralValue: Optional[float]
    tzsCollateralValue: Optional[float]
    restructuredLoans: int
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
    orgTotMonthlyPaymentAmount: float
    usdTotMonthlyPaymentAmount: float
    tzsTotMonthlyPaymentAmount: float
    orgInterestPaidTotal: Optional[float]
    usdInterestPaidTotal: Optional[float]
    tzsInterestPaidTotal: Optional[float]
    assetClassificationCategory: str
    sectorSnaClassification: str
    negStatusContract: Optional[str]
    customerRole: str
    allowanceProbableLoss: int
    botProvision: int

class OverdraftProcessor:
    """Processor for overdraft records"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def process_record(self, row, record_type='overdraft'):
        """Process a single overdraft record from DB2"""
        try:
            # Map the 69 fields from the SQL query to the dataclass
            record = OverdraftRecord(
                reportingDate=str(row[0]) if row[0] else '',
                custId=str(row[1]) if row[1] else '',
                accountNumber=str(row[2]) if row[2] else '',
                customerIdentificationNumber=str(row[3]) if row[3] else None,
                clientName=str(row[4]) if row[4] else '',
                clientType=str(row[5]) if row[5] else None,
                borrowerCountry=str(row[6]) if row[6] else None,
                ratingStatus=int(row[7]) if row[7] is not None else 0,
                crRatingBorrower=str(row[8]) if row[8] else None,
                gradesUnratedBanks=str(row[9]) if row[9] else None,
                groupCode=str(row[10]) if row[10] else None,
                relatedEntityName=str(row[11]) if row[11] else None,
                relatedParty=str(row[12]) if row[12] else None,
                relationshipCategory=str(row[13]) if row[13] else None,
                loanProductType=str(row[14]) if row[14] else None,
                idProduct=str(row[15]) if row[15] else None,
                overdraftEconomicActivity=str(row[16]) if row[16] else 'OtherServices',
                loanPhase=str(row[17]) if row[17] else 'TerminatedAccordingTheContract',
                transferStatus=str(row[18]) if row[18] else 'NotSpecified',
                purposeOtherLoans=str(row[19]) if row[19] else 'Other',
                contractDate=row[20] if row[20] else None,
                branchCode=str(row[21]) if row[21] else None,
                loanOfficer=str(row[22]) if row[22] else None,
                loanSupervisor=str(row[23]) if row[23] else None,
                currency=str(row[24]) if row[24] else '',
                orgSanctionedAmount=float(row[25]) if row[25] is not None else None,
                usdSanctionedAmount=float(row[26]) if row[26] is not None else None,
                tzsSanctionedAmount=float(row[27]) if row[27] is not None else None,
                orgUtilisedAmount=float(row[28]) if row[28] is not None else None,
                usdUtilisedAmount=float(row[29]) if row[29] is not None else None,
                tzsUtilisedAmount=float(row[30]) if row[30] is not None else None,
                orgCrUsageLast30DaysAmount=float(row[31]) if row[31] is not None else None,
                usdCrUsageLast30DaysAmount=float(row[32]) if row[32] is not None else None,
                tzsCrUsageLast30DaysAmount=float(row[33]) if row[33] is not None else None,
                disbursementDate=row[34] if row[34] else None,
                expiryDate=row[35] if row[35] else None,
                realEndDate=row[36] if row[36] else None,
                orgOutstandingAmount=float(row[37]) if row[37] is not None else None,
                usdOutstandingAmount=float(row[38]) if row[38] is not None else None,
                tzsOutstandingAmount=float(row[39]) if row[39] is not None else None,
                orgOutstandingPrincipalAmount=float(row[40]) if row[40] is not None else None,
                usdOutstandingPrincipalAmount=float(row[41]) if row[41] is not None else None,
                tzsOutstandingPrincipalAmount=float(row[42]) if row[42] is not None else None,
                latestCustomerCreditDate=row[43] if row[43] else None,
                latestCreditAmount=float(row[44]) if row[44] is not None else None,
                primeLendingRate=float(row[45]) if row[45] is not None else None,
                annualInterestRate=float(row[46]) if row[46] is not None else None,
                collateralPledged=str(row[47]) if row[47] else None,
                orgCollateralValue=float(row[48]) if row[48] is not None else None,
                usdCollateralValue=float(row[49]) if row[49] is not None else None,
                tzsCollateralValue=float(row[50]) if row[50] is not None else None,
                restructuredLoans=int(row[51]) if row[51] is not None else 0,
                orgAccruedInterestAmount=float(row[52]) if row[52] is not None else None,
                usdAccruedInterestAmount=float(row[53]) if row[53] is not None else None,
                tzsAccruedInterestAmount=float(row[54]) if row[54] is not None else None,
                orgPenaltyChargedAmount=float(row[55]) if row[55] is not None else None,
                usdPenaltyChargedAmount=float(row[56]) if row[56] is not None else None,
                tzsPenaltyChargedAmount=float(row[57]) if row[57] is not None else None,
                orgPenaltyPaidAmount=float(row[58]) if row[58] is not None else None,
                usdPenaltyPaidAmount=float(row[59]) if row[59] is not None else None,
                tzsPenaltyPaidAmount=float(row[60]) if row[60] is not None else None,
                orgLoanFeesChargedAmount=float(row[61]) if row[61] is not None else None,
                usdLoanFeesChargedAmount=float(row[62]) if row[62] is not None else None,
                tzsLoanFeesChargedAmount=float(row[63]) if row[63] is not None else None,
                orgLoanFeesPaidAmount=float(row[64]) if row[64] is not None else None,
                usdLoanFeesPaidAmount=float(row[65]) if row[65] is not None else None,
                tzsLoanFeesPaidAmount=float(row[66]) if row[66] is not None else None,
                orgTotMonthlyPaymentAmount=float(row[67]) if row[67] is not None else 0.0,
                usdTotMonthlyPaymentAmount=float(row[68]) if row[68] is not None else 0.0,
                tzsTotMonthlyPaymentAmount=float(row[69]) if row[69] is not None else 0.0,
                orgInterestPaidTotal=float(row[70]) if row[70] is not None else None,
                usdInterestPaidTotal=float(row[71]) if row[71] is not None else None,
                tzsInterestPaidTotal=float(row[72]) if row[72] is not None else None,
                assetClassificationCategory=str(row[73]) if row[73] else 'Current',
                sectorSnaClassification=str(row[74]) if row[74] else 'Other financial Corporations',
                negStatusContract=str(row[75]) if row[75] else None,
                customerRole=str(row[76]) if row[76] else 'N/A',
                allowanceProbableLoss=int(row[77]) if row[77] is not None else 0,
                botProvision=int(row[78]) if row[78] is not None else 0
            )
            
            return record
            
        except Exception as e:
            self.logger.error(f"Error processing overdraft record: {e}")
            self.logger.error(f"Row data: {row}")
            raise
    
    def validate_record(self, record):
        """Validate overdraft record"""
        try:
            # Basic validation
            if not record.accountNumber:
                self.logger.warning("Missing account number")
                return False
            
            if not record.custId:
                self.logger.warning("Missing customer ID")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating overdraft record: {e}")
            return False
    
    def insert_to_postgres(self, record, cursor):
        """Insert overdraft record to PostgreSQL"""
        try:
            insert_query = """
            INSERT INTO overdraft (
                reportingDate, custId, accountNumber, customerIdentificationNumber, clientName,
                clientType, borrowerCountry, ratingStatus, crRatingBorrower, gradesUnratedBanks,
                groupCode, relatedEntityName, relatedParty, relationshipCategory, loanProductType,
                idProduct, overdraftEconomicActivity, loanPhase, transferStatus, purposeOtherLoans,
                contractDate, branchCode, loanOfficer, loanSupervisor, currency,
                orgSanctionedAmount, usdSanctionedAmount, tzsSanctionedAmount,
                orgUtilisedAmount, usdUtilisedAmount, tzsUtilisedAmount,
                orgCrUsageLast30DaysAmount, usdCrUsageLast30DaysAmount, tzsCrUsageLast30DaysAmount,
                disbursementDate, expiryDate, realEndDate,
                orgOutstandingAmount, usdOutstandingAmount, tzsOutstandingAmount,
                orgOutstandingPrincipalAmount, usdOutstandingPrincipalAmount, tzsOutstandingPrincipalAmount,
                latestCustomerCreditDate, latestCreditAmount, primeLendingRate, annualInterestRate,
                collateralPledged, orgCollateralValue, usdCollateralValue, tzsCollateralValue,
                restructuredLoans, orgAccruedInterestAmount, usdAccruedInterestAmount, tzsAccruedInterestAmount,
                orgPenaltyChargedAmount, usdPenaltyChargedAmount, tzsPenaltyChargedAmount,
                orgPenaltyPaidAmount, usdPenaltyPaidAmount, tzsPenaltyPaidAmount,
                orgLoanFeesChargedAmount, usdLoanFeesChargedAmount, tzsLoanFeesChargedAmount,
                orgLoanFeesPaidAmount, usdLoanFeesPaidAmount, tzsLoanFeesPaidAmount,
                orgTotMonthlyPaymentAmount, usdTotMonthlyPaymentAmount, tzsTotMonthlyPaymentAmount,
                orgInterestPaidTotal, usdInterestPaidTotal, tzsInterestPaidTotal,
                assetClassificationCategory, sectorSnaClassification, negStatusContract,
                customerRole, allowanceProbableLoss, botProvision
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            
            cursor.execute(insert_query, (
                record.reportingDate, record.custId, record.accountNumber, record.customerIdentificationNumber,
                record.clientName, record.clientType, record.borrowerCountry, record.ratingStatus,
                record.crRatingBorrower, record.gradesUnratedBanks, record.groupCode, record.relatedEntityName,
                record.relatedParty, record.relationshipCategory, record.loanProductType, record.idProduct,
                record.overdraftEconomicActivity, record.loanPhase, record.transferStatus, record.purposeOtherLoans,
                record.contractDate, record.branchCode, record.loanOfficer, record.loanSupervisor, record.currency,
                record.orgSanctionedAmount, record.usdSanctionedAmount, record.tzsSanctionedAmount,
                record.orgUtilisedAmount, record.usdUtilisedAmount, record.tzsUtilisedAmount,
                record.orgCrUsageLast30DaysAmount, record.usdCrUsageLast30DaysAmount, record.tzsCrUsageLast30DaysAmount,
                record.disbursementDate, record.expiryDate, record.realEndDate,
                record.orgOutstandingAmount, record.usdOutstandingAmount, record.tzsOutstandingAmount,
                record.orgOutstandingPrincipalAmount, record.usdOutstandingPrincipalAmount, record.tzsOutstandingPrincipalAmount,
                record.latestCustomerCreditDate, record.latestCreditAmount, record.primeLendingRate, record.annualInterestRate,
                record.collateralPledged, record.orgCollateralValue, record.usdCollateralValue, record.tzsCollateralValue,
                record.restructuredLoans, record.orgAccruedInterestAmount, record.usdAccruedInterestAmount, record.tzsAccruedInterestAmount,
                record.orgPenaltyChargedAmount, record.usdPenaltyChargedAmount, record.tzsPenaltyChargedAmount,
                record.orgPenaltyPaidAmount, record.usdPenaltyPaidAmount, record.tzsPenaltyPaidAmount,
                record.orgLoanFeesChargedAmount, record.usdLoanFeesChargedAmount, record.tzsLoanFeesChargedAmount,
                record.orgLoanFeesPaidAmount, record.usdLoanFeesPaidAmount, record.tzsLoanFeesPaidAmount,
                record.orgTotMonthlyPaymentAmount, record.usdTotMonthlyPaymentAmount, record.tzsTotMonthlyPaymentAmount,
                record.orgInterestPaidTotal, record.usdInterestPaidTotal, record.tzsInterestPaidTotal,
                record.assetClassificationCategory, record.sectorSnaClassification, record.negStatusContract,
                record.customerRole, record.allowanceProbableLoss, record.botProvision
            ))
            
        except Exception as e:
            self.logger.error(f"Error inserting overdraft record to PostgreSQL: {e}")
            raise