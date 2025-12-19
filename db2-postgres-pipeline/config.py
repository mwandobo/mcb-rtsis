"""
Configuration management for DB2 to PostgreSQL pipeline
"""

import os
from dataclasses import dataclass
from typing import Dict, List
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    # DB2 Configuration for ibm_db
    db2_host: str = os.getenv('DB2_HOST', '172.10.2.42')
    db2_port: int = int(os.getenv('DB2_PORT', '50000'))
    db2_database: str = os.getenv('DB2_DATABASE', 'mcbecho')
    db2_user: str = os.getenv('DB2_USER', 'profits')
    db2_password: str = os.getenv('DB2_PASSWORD', 'prft2016')
    db2_schema: str = os.getenv('DB2_SCHEMA', 'PROFITS')
    
    # PostgreSQL Configuration
    pg_host: str = os.getenv('PG_HOST', 'localhost')
    pg_port: int = int(os.getenv('PG_PORT', '5432'))
    pg_database: str = os.getenv('PG_DATABASE', 'bankdb')
    pg_user: str = os.getenv('PG_USER', 'postgres')
    pg_password: str = os.getenv('PG_PASSWORD', 'postgres')

@dataclass
class MessageQueueConfig:
    """Message queue configuration"""
    # RabbitMQ Configuration
    rabbitmq_host: str = os.getenv('RABBITMQ_HOST', 'localhost')
    rabbitmq_port: int = int(os.getenv('RABBITMQ_PORT', '5672'))
    rabbitmq_user: str = os.getenv('RABBITMQ_USER', 'guest')
    rabbitmq_password: str = os.getenv('RABBITMQ_PASSWORD', 'guest')
    
    # Redis Configuration
    redis_host: str = os.getenv('REDIS_HOST', 'localhost')
    redis_port: int = int(os.getenv('REDIS_PORT', '6379'))
    redis_db: int = int(os.getenv('REDIS_DB', '0'))

@dataclass
class PipelineConfig:
    """Pipeline processing configuration"""
    poll_interval: int = int(os.getenv('POLL_INTERVAL', '10'))
    batch_size: int = int(os.getenv('BATCH_SIZE', '1000'))
    max_retries: int = int(os.getenv('MAX_RETRIES', '3'))
    retry_delay: int = int(os.getenv('RETRY_DELAY', '5'))
    
    # Logging
    log_level: str = os.getenv('LOG_LEVEL', 'INFO')
    log_file: str = os.getenv('LOG_FILE', 'logs/pipeline.log')

@dataclass
class TableConfig:
    """Configuration for individual table processing"""
    name: str
    query: str
    timestamp_column: str
    target_table: str
    queue_name: str
    processor_class: str
    batch_size: int = 1000
    poll_interval: int = 10

class Config:
    """Main configuration class"""
    
    def __init__(self):
        self.database = DatabaseConfig()
        self.message_queue = MessageQueueConfig()
        self.pipeline = PipelineConfig()
        self.tables = self._get_table_configs()
    
    def _get_table_configs(self) -> Dict[str, TableConfig]:
        """Define table configurations"""
        return {
            'cash_information': TableConfig(
                name='cash_information',
                query="""
                SELECT
                    CURRENT_TIMESTAMP as reportingDate,
                    gte.FK_UNITCODETRXUNIT AS branchCode,
                    CASE
                      WHEN gl.EXTERNAL_GLACCOUNT='101000001' THEN 'Cash in vault'
                      WHEN gl.EXTERNAL_GLACCOUNT='101000002' THEN 'Petty cash'
                      WHEN gl.EXTERNAL_GLACCOUNT='101000010' OR gl.EXTERNAL_GLACCOUNT='101000015' THEN 'Cash in ATMs'
                      WHEN gl.EXTERNAL_GLACCOUNT='101000004' OR gl.EXTERNAL_GLACCOUNT='101000011' THEN 'Cash in Teller'
                      ELSE 'unknown'
                    END as cashCategory,
                    null as cashSubCategory,
                    'Business Hours' as cashSubmissionTime,
                    gte.CURRENCY_SHORT_DES as currency,
                    null as cashDenomination,
                    null as quantityOfCoinsNotes,
                    gte.DC_AMOUNT AS orgAmount,
                    CASE
                        WHEN gte.CURRENCY_SHORT_DES = 'USD'
                            THEN gte.DC_AMOUNT
                        ELSE NULL
                    END AS usdAmount,
                    CASE
                        WHEN gte.CURRENCY_SHORT_DES = 'USD'
                            THEN gte.DC_AMOUNT * 2500
                        ELSE
                            gte.DC_AMOUNT
                    END AS tzsAmount,
                    gte.TRN_DATE as transactionDate,
                    gte.AVAILABILITY_DATE as maturityDate,
                    0 as allowanceProbableLoss,
                    0 as botProvision
                FROM GLI_TRX_EXTRACT AS gte
                JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
                WHERE gl.EXTERNAL_GLACCOUNT IN ('101000001','101000002','101000004','101000007','101000010','101000015')
                ORDER BY gte.TRN_DATE
                FETCH FIRST 1000 ROWS ONLY
                """,
                timestamp_column='TRN_DATE',
                target_table='cash_information',
                queue_name='cash_information_queue',
                processor_class='CashProcessor',
                batch_size=1000,
                poll_interval=10
            ),
            'asset_owned': TableConfig(
                name='asset_owned',
                query="""
                SELECT
                    CURRENT_TIMESTAMP AS reportingDate,
                    M.AQUISITION_DATE as acquisitionDate,
                    CU.SHORT_DESCR as currency,
                    CASE
                        WHEN gl.EXTERNAL_GLACCOUNT = '144000020' OR
                             gl.EXTERNAL_GLACCOUNT = '144000052' OR
                             gl.EXTERNAL_GLACCOUNT = '170150001' OR
                             gl.EXTERNAL_GLACCOUNT = '171030001' OR
                             gl.EXTERNAL_GLACCOUNT = '170150002' THEN 'Intangible'
                        WHEN gl.EXTERNAL_GLACCOUNT = '171020001' THEN 'Immovable'
                        WHEN gl.EXTERNAL_GLACCOUNT = '170050001' OR
                             gl.EXTERNAL_GLACCOUNT = '170120001' OR
                             gl.EXTERNAL_GLACCOUNT = '171090001' OR
                             gl.EXTERNAL_GLACCOUNT = '161020001' OR
                             gl.EXTERNAL_GLACCOUNT = '170090001' OR
                             gl.EXTERNAL_GLACCOUNT = '170070001' THEN 'Movable'
                        ELSE 'Other'
                    END AS assetCategory,
                    CASE
                        WHEN M.GL_ACCOUNT = '1.7.0.12.0001' THEN 'Computer'
                        WHEN M.GL_ACCOUNT = '1.7.0.09.0001' THEN 'Motor Vehicle'
                        WHEN M.GL_ACCOUNT = '1.7.0.07.0001' THEN 'Machinery And Equipment'
                        WHEN M.GL_ACCOUNT = '1.7.0.05.0001' THEN 'Furniture and fittings'
                        WHEN M.GL_ACCOUNT = '1.6.1.02.0001' THEN 'Buildings and improvements'
                        WHEN M.GL_ACCOUNT = '1.7.0.15.0001' THEN 'Infrastructure investments'
                        ELSE 'Other'
                    END AS assetType,
                    M.AMOUNT AS orgCostValue,
                    CASE
                        WHEN CU.SHORT_DESCR = 'USD'
                            THEN M.AMOUNT
                        ELSE NULL
                    END AS usdCostValue,
                    CASE
                        WHEN CU.SHORT_DESCR = 'USD'
                            THEN M.AMOUNT * 2500
                        ELSE
                            M.AMOUNT
                    END AS tzsCostValue,
                    0 as allowanceProbableLoss,
                    0 as botProvision
                FROM ASSET_MASTER AS M
                LEFT JOIN CURRENCY as CU ON CU.ID_CURRENCY = M.CURRENCY_ID
                LEFT JOIN GLG_ACCOUNT AS gl ON gl.ACCOUNT_ID = M.GL_ACCOUNT
                ORDER BY M.AQUISITION_DATE
                FETCH FIRST 1000 ROWS ONLY
                """,
                timestamp_column='AQUISITION_DATE',
                target_table='asset_owned',
                queue_name='asset_owned_queue',
                processor_class='AssetsProcessor',
                batch_size=1000,
                poll_interval=10
            ),
            'balances_bot': TableConfig(
                name='balances_bot',
                query="""
                SELECT CURRENT_TIMESTAMP as reportingDate,
                    gte.FK_GLG_ACCOUNTACCO as accountNumber,
                    'BANK OF TANZANIA' as accountName,
                    'TIPS' as accountType,
                    null as subAccountType,
                    gte.CURRENCY_SHORT_DES as currency,
                    gte.DC_AMOUNT AS orgAmount,
                    CASE
                        WHEN gte.CURRENCY_SHORT_DES = 'USD'
                            THEN gte.DC_AMOUNT
                        ELSE NULL
                    END AS usdAmount,
                    CASE
                        WHEN gte.CURRENCY_SHORT_DES = 'USD'
                            THEN gte.DC_AMOUNT * 2500
                        ELSE
                            gte.DC_AMOUNT
                    END AS tzsAmount,
                    gte.TRN_DATE as transactionDate,
                    CURRENT_TIMESTAMP as maturityDate,
                    0 as allowanceProbableLoss,
                    0 as botProvision
                FROM GLI_TRX_EXTRACT AS gte
                JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
                JOIN CUSTOMER c ON c.CUST_ID = gte.CUST_ID
                LEFT JOIN CURRENCY cu ON UPPER(TRIM(cu.SHORT_DESCR)) = UPPER(TRIM(gte.CURRENCY_SHORT_DES))
                WHERE gl.EXTERNAL_GLACCOUNT='100028000'
                ORDER BY gte.TRN_DATE
                FETCH FIRST 1000 ROWS ONLY
                """,
                timestamp_column='TRN_DATE',
                target_table='balances_bot',
                queue_name='balances_bot_queue',
                processor_class='BotBalancesProcessor',
                batch_size=1000,
                poll_interval=10
            ),
            'balances_with_mnos': TableConfig(
                name='balances_with_mnos',
                query="""
                SELECT
                    CURRENT_TIMESTAMP AS reportingDate,
                    CURRENT_TIMESTAMP AS floatBalanceDate,
                    CASE gl.EXTERNAL_GLACCOUNT
                        WHEN '504080001' THEN 'Super Agent Commission'
                        WHEN '144000051' THEN 'AIRTEL Money Super Agent Float'
                        WHEN '144000058' THEN 'TIGO PESA Super Agent Float'
                        WHEN '144000061' THEN 'HALOPESA Super Agent Float'
                        WHEN '144000062' THEN 'MPESA Super Agent Float'
                        ELSE ''
                    END AS mnoCode,
                    gte.FK_GLG_ACCOUNTACCO AS tillNumber,
                    gte.CURRENCY_SHORT_DES AS currency,
                    0 AS allowanceProbableLoss,
                    0 AS botProvision,
                    gte.DC_AMOUNT AS orgFloatAmount,
                    CASE
                        WHEN gte.CURRENCY_SHORT_DES = 'USD'
                            THEN gte.DC_AMOUNT
                        ELSE NULL
                    END AS usdFloatAmount,
                    CASE
                        WHEN gte.CURRENCY_SHORT_DES = 'USD'
                            THEN gte.DC_AMOUNT * 2500
                        ELSE
                            gte.DC_AMOUNT
                    END AS tzsFloatAmount
                FROM GLI_TRX_EXTRACT gte
                JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
                WHERE gl.EXTERNAL_GLACCOUNT IN ('504080001','144000051','144000058','144000061','144000062')
                ORDER BY gte.TRN_DATE
                FETCH FIRST 1000 ROWS ONLY
                """,
                timestamp_column='TRN_DATE',
                target_table='balances_with_mnos',
                queue_name='balances_with_mnos_queue',
                processor_class='MnosProcessor',
                batch_size=1000,
                poll_interval=10
            ),
            'balance_with_other_banks': TableConfig(
                name='balance_with_other_banks',
                query="""
                SELECT
                    CURRENT_TIMESTAMP AS reportingDate,
                    gte.FK_GLG_ACCOUNTACCO as accountNumber,
                    c.FIRST_NAME as accountName,
                    CASE
                        WHEN UPPER(c.FIRST_NAME) = 'ECOBANK' THEN 'ECOCTZTZXXX'
                        WHEN UPPER(c.FIRST_NAME) = 'BOA' THEN 'EUAFTZTZXXX'
                        WHEN UPPER(c.FIRST_NAME) = 'TPB' THEN 'TAPBTZTZ'
                        WHEN UPPER(c.FIRST_NAME) = 'TANZANIA POSTAL BANK' THEN 'TAPBTZTZXX'
                        ELSE VARCHAR(gte.FK0UNITCODE)
                    END AS bankCode,
                    'Tanzania' as Country,
                    'Domestic bank related' as relationshipType,
                    '' as accountType,
                    '' as subAccountType,
                    gte.CURRENCY_SHORT_DES as currency,
                    gte.DC_AMOUNT AS orgAmount,
                    CASE
                        WHEN gte.CURRENCY_SHORT_DES = 'USD'
                            THEN gte.DC_AMOUNT
                        ELSE NULL
                    END AS usdAmount,
                    CASE
                        WHEN gte.CURRENCY_SHORT_DES = 'USD'
                            THEN gte.DC_AMOUNT * 2500
                        ELSE
                            gte.DC_AMOUNT
                    END AS tzsAmount,
                    gte.TRN_DATE as transactionDate,
                    (DATE(gte.AVAILABILITY_DATE) - DATE(gte.TRN_DATE)) AS pastDueDays,
                    0 as allowanceProbableLoss,
                    0 as botProvision,
                    'Current' as assetsClassificationCategory,
                    gte.TRN_DATE as contractDate,
                    gte.AVAILABILITY_DATE as maturityDate,
                    'Highly rated Multilateral Development Banks' as externalRatingCorrespondentBank,
                    '' as gradesUnratedBanks
                FROM GLI_TRX_EXTRACT as gte
                JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
                JOIN CUSTOMER c ON gte.CUST_ID = c.CUST_ID
                WHERE gl.EXTERNAL_GLACCOUNT IN('100050001')
                ORDER BY gte.TRN_DATE
                FETCH FIRST 1000 ROWS ONLY
                """,
                timestamp_column='TRN_DATE',
                target_table='balance_with_other_bank',
                queue_name='balance_with_other_banks_queue',
                processor_class='OtherBanksProcessor',
                batch_size=1000,
                poll_interval=10
            ),
            'other_assets': TableConfig(
                name='other_assets',
                query="""
                SELECT
                    (SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1) AS reportingDate,
                    CASE
                        WHEN gl.EXTERNAL_GLACCOUNT = '100017000' THEN 'Gold'
                        WHEN gl.EXTERNAL_GLACCOUNT = '144000032' THEN 'StampAccount'
                        WHEN gl.EXTERNAL_GLACCOUNT IN ('144000015','144000047','144000048','144000050',
                                                       '144000051','144000054','144000058','144000061',
                                                       '144000062','144000074') THEN 'SundryDebtors'
                        WHEN gl.EXTERNAL_GLACCOUNT IN ('230000007','230000071','145000001','230000079')
                                                                                    THEN 'Trade Credit and advances'
                        WHEN gl.EXTERNAL_GLACCOUNT IN ('144000006','144000066') THEN 'PrepaidExpenses'
                        WHEN gl.EXTERNAL_GLACCOUNT IN ('170150001','170150002','171030001','144000052')
                                                                                    THEN 'otherIntangible asset'
                        ELSE 'MiscellaneousAssets'
                    END AS assetType,
                    gte.TRN_DATE AS transactionDate,
                    gte.AVAILABILITY_DATE AS maturityDate,
                    TRIM(RTRIM(WC.FIRST_NAME) || ' ' ||
                         COALESCE(RTRIM(WC.MIDDLE_NAME), '') || ' ' ||
                         RTRIM(WC.SURNAME)) AS debtorName,
                    CASE WHEN TRIM(WC.NATIONAL_DESCRIPTION) LIKE '%TANZANIAN%' THEN 'Tanzania' ELSE 'Unknown' END
                                                                                AS debtorCountry,
                    gte.CURRENCY_SHORT_DES AS currency,
                    gte.DC_AMOUNT AS orgAmount,
                    CASE WHEN gte.CURRENCY_SHORT_DES = 'USD'
                         THEN gte.DC_AMOUNT
                         ELSE NULL
                    END AS usdAmount,
                    CASE WHEN gte.CURRENCY_SHORT_DES = 'USD'
                         THEN gte.DC_AMOUNT * 2730.50
                         ELSE gte.DC_AMOUNT
                    END AS tzsAmount,
                    CASE
                        WHEN gl.EXTERNAL_GLACCOUNT IN ('144000001','144000039','144000043','101010001','101240001')
                            THEN 'Other Depository Corporations'
                        WHEN gl.EXTERNAL_GLACCOUNT IN ('144000047','144000048','144000050','144000051',
                                                       '144000058','144000061','144000062','144000074')
                            THEN 'Other Financial Intermediary'
                        WHEN gl.EXTERNAL_GLACCOUNT IN ('230000007','145000001','230000071') THEN 'Households'
                        WHEN gl.EXTERNAL_GLACCOUNT = '112020005' THEN 'Insurance Companies'
                        WHEN gl.EXTERNAL_GLACCOUNT = '143000001' THEN 'Central Government'
                        WHEN gl.EXTERNAL_GLACCOUNT = '100017000' THEN 'Other Non-Financial Corporations'
                        WHEN COALESCE(WC.CUST_ID,'') != ''
                         AND TRIM(UPPER(WC.NATIONAL_DESCRIPTION)) LIKE '%TANZANIAN%' THEN 'Households'
                        WHEN COALESCE(WC.CUST_ID,'') != '' THEN 'Other Non-Financial Corporations'
                        ELSE 'Other Non-Financial Corporations'
                    END AS sectorSnaClassification,
                    CASE
                        WHEN gte.AVAILABILITY_DATE IS NULL THEN NULL
                        WHEN gte.AVAILABILITY_DATE < (SELECT CURRENT DATE FROM SYSIBM.SYSDUMMY1)
                            THEN DAYS( (SELECT CURRENT DATE FROM SYSIBM.SYSDUMMY1) )
                                 - DAYS(gte.AVAILABILITY_DATE)
                        ELSE 0
                    END AS pastDueDays,
                    CASE
                        WHEN gte.AVAILABILITY_DATE IS NULL THEN 1
                        WHEN DAYS( (SELECT CURRENT DATE FROM SYSIBM.SYSDUMMY1) )
                             - DAYS(gte.AVAILABILITY_DATE) <= 30 THEN 1
                        WHEN DAYS( (SELECT CURRENT DATE FROM SYSIBM.SYSDUMMY1) )
                             - DAYS(gte.AVAILABILITY_DATE) <= 90 THEN 2
                        ELSE 3
                    END AS assetClassificationCategory,
                    0 AS allowanceProbableLoss,
                    0 AS botProvision
                FROM GLI_TRX_EXTRACT AS gte
                LEFT JOIN CUSTOMER AS C ON C.CUST_ID = gte.CUST_ID
                LEFT JOIN W_DIM_CUSTOMER AS WC ON WC.CUST_ID = gte.CUST_ID
                LEFT JOIN GLG_ACCOUNT AS gl ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO
                WHERE gl.EXTERNAL_GLACCOUNT IN (
                    '100017000','101010001','101240001','112020005','143000001',
                    '144000001','144000015','144000032','144000039','144000043',
                    '144000046','144000047','144000048','144000050','144000051',
                    '144000054','144000057','144000058','144000061','144000062',
                    '144000066','144000074','145000001','230000007','230000013',
                    '230000014','230000071','230000079','144000006','144000052',
                    '170150001','170150002','171030001','705190001','705190002',
                    '705190003','144000020'
                )
                ORDER BY gte.TRN_DATE
                FETCH FIRST 1000 ROWS ONLY
                """,
                timestamp_column='TRN_DATE',
                target_table='other_assets',
                queue_name='other_assets_queue',
                processor_class='OtherAssetsProcessor',
                batch_size=1000,
                poll_interval=10
            ),
            'overdraft': TableConfig(
                name='overdraft',
                query="""
                SELECT 
                    VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHH24MI') AS reportingDate,
                    'OD001' AS accountNumber,
                    'CID001' AS customerIdentificationNumber,
                    'Sample Overdraft Client' AS clientName,
                    'Individual' AS clientType,
                    'Tanzania' as borrowerCountry,
                    CAST(NULL AS VARCHAR(50)) as ratingStatus,
                    CAST(NULL AS VARCHAR(50)) as crRatingBorrower,
                    CAST(NULL AS VARCHAR(50)) as gradesUnratedBanks,
                    CAST(NULL AS VARCHAR(50)) as groupCode,
                    CAST(NULL AS VARCHAR(200)) as relatedEntityName,
                    CAST(NULL AS VARCHAR(50)) as relatedParty,
                    CAST(NULL AS VARCHAR(50)) as relationshipCategory,
                    'Business Overdraft' as loanProductType,
                    'OtherServices' as overdraftEconomicActivity,
                    'Existing' as loanPhase,
                    'NotSpecified' as transferStatus,
                    'Business Overdraft' AS purposeOtherLoans,
                    CURRENT_DATE - 30 DAYS as contractDate,
                    '001' as branchCode,
                    'Sample Officer' as loanOfficer,
                    CAST(NULL AS VARCHAR(200)) as loanSupervisor,
                    'TZS' as currency,
                    1000000.00 as orgSanctionedAmount,
                    CAST(NULL AS DECIMAL(15,2)) AS usdSanctionedAmount,
                    1000000.00 AS tzsSanctionedAmount,
                    500000.00 as orgUtilisedAmount,
                    CAST(NULL AS DECIMAL(15,2)) AS usdUtilisedAmount,
                    500000.00 AS tzsUtilisedAmount,
                    50000.00 as orgCrUsageLast30DaysAmount,
                    CAST(NULL AS DECIMAL(15,2)) AS usdCrUsageLast30DaysAmount,
                    50000.00 AS tzsCrUsageLast30DaysAmount,
                    CURRENT_DATE - 30 DAYS AS disbursementDate,
                    CURRENT_DATE + 365 DAYS AS expiryDate,
                    CURRENT_DATE + 365 DAYS AS realEndDate,
                    450000.00 AS orgOutstandingAmount,
                    CAST(NULL AS DECIMAL(15,2)) AS usdOutstandingAmount,
                    450000.00 AS tzsOutstandingAmount,
                    CURRENT_DATE - 30 DAYS AS latestCustomerCreditDate,
                    100000.00 AS latestCreditAmount,
                    15.50 AS primeLendingRate,
                    18.00 AS annualInterestRate,
                    800000.00 AS collateralPledged,
                    800000.00 AS orgCollateralValue,
                    CAST(NULL AS DECIMAL(15,2)) AS usdCollateralValue,
                    800000.00 AS tzsCollateralValue,
                    0 AS restructuredLoans,
                    0 AS pastDueDays,
                    0.00 AS pastDueAmount,
                    5000.00 AS orgAccruedInterestAmount,
                    CAST(NULL AS DECIMAL(15,2)) AS usdAccruedInterestAmount,
                    5000.00 AS tzsAccruedInterestAmount,
                    0.00 AS orgPenaltyChargedAmount,
                    CAST(NULL AS DECIMAL(15,2)) AS usdPenaltyChargedAmount,
                    0.00 AS tzsPenaltyChargedAmount,
                    2500.00 AS orgLoanFeesChargedAmount,
                    CAST(NULL AS DECIMAL(15,2)) AS usdLoanFeesChargedAmount,
                    2500.00 AS tzsLoanFeesChargedAmount,
                    2500.00 AS orgLoanFeesPaidAmount,
                    CAST(NULL AS DECIMAL(15,2)) AS usdLoanFeesPaidAmount,
                    2500.00 AS tzsLoanFeesPaidAmount,
                    25000.00 AS orgTotMonthlyPaymentAmount,
                    CAST(NULL AS DECIMAL(15,2)) AS usdTotMonthlyPaymentAmount,
                    25000.00 AS tzsTotMonthlyPaymentAmount,
                    15000.00 AS orgInterestPaidTotal,
                    CAST(NULL AS DECIMAL(15,2)) AS usdInterestPaidTotal,
                    15000.00 AS tzsInterestPaidTotal,
                    'Current' AS assetClassificationCategory,
                    'Other Financial Intermediary' AS sectorSnaClassification,
                    'Active' AS negStatusContract,
                    'Individual' AS customerRole,
                    0.00 AS allowanceProbableLoss,
                    0.00 AS botProvision
                FROM SYSIBM.SYSDUMMY1
                UNION ALL
                SELECT 
                    VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHH24MI') AS reportingDate,
                    'OD002' AS accountNumber,
                    'CID002' AS customerIdentificationNumber,
                    'Sample Business Client' AS clientName,
                    'Corporate' AS clientType,
                    'Tanzania' as borrowerCountry,
                    CAST(NULL AS VARCHAR(50)) as ratingStatus,
                    CAST(NULL AS VARCHAR(50)) as crRatingBorrower,
                    CAST(NULL AS VARCHAR(50)) as gradesUnratedBanks,
                    CAST(NULL AS VARCHAR(50)) as groupCode,
                    CAST(NULL AS VARCHAR(200)) as relatedEntityName,
                    CAST(NULL AS VARCHAR(50)) as relatedParty,
                    CAST(NULL AS VARCHAR(50)) as relationshipCategory,
                    'Business Overdraft' as loanProductType,
                    'OtherServices' as overdraftEconomicActivity,
                    'Existing' as loanPhase,
                    'NotSpecified' as transferStatus,
                    'Business Overdraft' AS purposeOtherLoans,
                    CURRENT_DATE - 60 DAYS as contractDate,
                    '002' as branchCode,
                    'Business Officer' as loanOfficer,
                    CAST(NULL AS VARCHAR(200)) as loanSupervisor,
                    'USD' as currency,
                    2000.00 as orgSanctionedAmount,
                    2000.00 AS usdSanctionedAmount,
                    5000000.00 AS tzsSanctionedAmount,
                    1200.00 as orgUtilisedAmount,
                    1200.00 AS usdUtilisedAmount,
                    3000000.00 AS tzsUtilisedAmount,
                    200.00 as orgCrUsageLast30DaysAmount,
                    200.00 AS usdCrUsageLast30DaysAmount,
                    500000.00 AS tzsCrUsageLast30DaysAmount,
                    CURRENT_DATE - 60 DAYS AS disbursementDate,
                    CURRENT_DATE + 300 DAYS AS expiryDate,
                    CURRENT_DATE + 300 DAYS AS realEndDate,
                    1100.00 AS orgOutstandingAmount,
                    1100.00 AS usdOutstandingAmount,
                    2750000.00 AS tzsOutstandingAmount,
                    CURRENT_DATE - 60 DAYS AS latestCustomerCreditDate,
                    500.00 AS latestCreditAmount,
                    15.50 AS primeLendingRate,
                    20.00 AS annualInterestRate,
                    3000.00 AS collateralPledged,
                    3000.00 AS orgCollateralValue,
                    3000.00 AS usdCollateralValue,
                    7500000.00 AS tzsCollateralValue,
                    0 AS restructuredLoans,
                    15 AS pastDueDays,
                    100.00 AS pastDueAmount,
                    50.00 AS orgAccruedInterestAmount,
                    50.00 AS usdAccruedInterestAmount,
                    125000.00 AS tzsAccruedInterestAmount,
                    25.00 AS orgPenaltyChargedAmount,
                    25.00 AS usdPenaltyChargedAmount,
                    62500.00 AS tzsPenaltyChargedAmount,
                    100.00 AS orgLoanFeesChargedAmount,
                    100.00 AS usdLoanFeesChargedAmount,
                    250000.00 AS tzsLoanFeesChargedAmount,
                    100.00 AS orgLoanFeesPaidAmount,
                    100.00 AS usdLoanFeesPaidAmount,
                    250000.00 AS tzsLoanFeesPaidAmount,
                    150.00 AS orgTotMonthlyPaymentAmount,
                    150.00 AS usdTotMonthlyPaymentAmount,
                    375000.00 AS tzsTotMonthlyPaymentAmount,
                    300.00 AS orgInterestPaidTotal,
                    300.00 AS usdInterestPaidTotal,
                    750000.00 AS tzsInterestPaidTotal,
                    'Watch' AS assetClassificationCategory,
                    'Other Non-Financial Corporations' AS sectorSnaClassification,
                    'Active' AS negStatusContract,
                    'Corporate' AS customerRole,
                    50.00 AS allowanceProbableLoss,
                    25.00 AS botProvision
                FROM SYSIBM.SYSDUMMY1
                """,
                timestamp_column='contractDate',
                target_table='overdraft',
                queue_name='overdraft_queue',
                processor_class='OverdraftProcessor',
                batch_size=1000,
                poll_interval=10
            ),
            'branch': TableConfig(
                name='branch',
                query="""
                SELECT
                    VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate,
                    COALESCE(u.UNIT_NAME, u.UNIT_NAME_LATIN, 'Unknown Branch') AS branchName,
                    COALESCE(bp.BANK_COMPANY_RECOR, '123456789') AS taxIdentificationNumber,
                    COALESCE(gd_license.LATIN_DESC, 'TL-' || CAST(u.CODE AS VARCHAR(10))) AS businessLicense,
                    CAST(u.CODE AS VARCHAR(10)) AS branchCode,
                    COALESCE(gd_fsr.LATIN_DESC, 'FSR-' || CAST(u.CODE AS VARCHAR(10))) AS qrFsrCode,
                    COALESCE(gd_region.LATIN_DESC, gd_region.DESCRIPTION, 'Unknown Region') AS region,
                    COALESCE(bd.DESCRIPTION, gd_district.LATIN_DESC, 'Unknown District') AS district,
                    COALESCE(gd_ward.LATIN_DESC, 'Unknown Ward') AS ward,
                    COALESCE(u.LC_STREET_NAME, u.ADDRESS, u.ADDRESS_LATIN, u.ADDRESS_2) AS street,
                    COALESCE(u.PLOT_STREET, u.BUILDING_UNIT) AS houseNumber,
                    COALESCE(u.ZIP_CODE, u.PO_BOX) AS postalCode,
                    CASE
                        WHEN u.LATITUDE_LOCATION IS NOT NULL AND u.LONGITUDE_LOCATION IS NOT NULL
                            THEN TRIM(u.LATITUDE_LOCATION) || ',' || TRIM(u.LONGITUDE_LOCATION)
                        WHEN u.GEO_AREA IS NOT NULL
                            THEN u.GEO_AREA
                        ELSE '0.0000,0.0000'
                    END AS gpsCoordinates,
                    COALESCE(gd_services.LATIN_DESC,
                        CASE WHEN u.CS_UNIT = '1' THEN 'Full Banking Services'
                        ELSE 'Limited Banking Services' END) AS bankingServices,
                    COALESCE(gd_mobile.LATIN_DESC, 'Not Available') AS mobileMoneyServices,
                    VARCHAR_FORMAT(COALESCE(u.OPEN_DATE, DATE(u.TMSTAMP), CURRENT_DATE), 'DDMMYYYYHHMM') AS registrationDate,
                    CASE
                        WHEN u.ENTRY_STATUS = '1' AND u.INACTIVE_UNIT = '0' THEN 'Active'
                        WHEN u.ENTRY_STATUS = '1' AND u.INACTIVE_UNIT = '1' THEN 'Inactive'
                        WHEN u.ENTRY_STATUS = '0' THEN 'Closed'
                        ELSE 'Unknown'
                    END AS branchStatus,
                    CASE
                        WHEN u.ENTRY_STATUS = '0' OR u.INACTIVE_UNIT = '1'
                            THEN VARCHAR_FORMAT(CURRENT_DATE, 'DDMMYYYYHHMM')
                        ELSE NULL
                    END AS closureDate,
                    COALESCE(emp.FIRST_NAME || ' ' || emp.LAST_NAME, 'Branch Manager') AS contactPerson,
                    COALESCE(u.TELEPHONE_1, u.TELEPHONE_2, '255000000000') AS telephoneNumber,
                    CASE
                        WHEN u.TELEPHONE_1 IS NOT NULL AND u.TELEPHONE_2 IS NOT NULL
                            AND u.TELEPHONE_1 != u.TELEPHONE_2
                            THEN u.TELEPHONE_2
                        ELSE u.FAX
                    END AS altTelephoneNumber,
                    COALESCE(gd_category.LATIN_DESC,
                        CASE
                            WHEN u.CS_HEAD_UNIT IS NULL THEN 'Head Office'
                            WHEN u.CS_UNIT = '1' THEN 'Full Service Branch'
                            ELSE 'Sub Branch'
                        END) AS branchCategory,
                    u.TMSTAMP AS lastModified
                FROM UNIT u
                LEFT JOIN BANK_PARAMETERS bp ON 1=1
                LEFT JOIN BDG_DISTRICT bd ON bd.ID = u.FK_BDG_DISTRICTID
                LEFT JOIN BANKEMPLOYEE emp ON emp.ID = (
                    SELECT MIN(be.ID) FROM BANKEMPLOYEE be WHERE be.EMPL_STATUS = '1'
                )
                LEFT JOIN GENERIC_DETAIL gd_region ON gd_region.FK_GENERIC_HEADPAR = u.FKGH_RESIDES_IN_RE
                    AND gd_region.SERIAL_NUM = u.FKGD_RESIDES_IN_RE AND gd_region.ENTRY_STATUS = '1'
                LEFT JOIN GENERIC_DETAIL gd_district ON gd_district.FK_GENERIC_HEADPAR = u.FKGH_RESIDES_IN_R1
                    AND gd_district.SERIAL_NUM = u.FKGD_RESIDES_IN_R1 AND gd_district.ENTRY_STATUS = '1'
                LEFT JOIN GENERIC_DETAIL gd_ward ON gd_ward.FK_GENERIC_HEADPAR = u.FKGH_RESID_REGION3
                    AND gd_ward.SERIAL_NUM = u.FKGD_RESID_REGION3 AND gd_ward.ENTRY_STATUS = '1'
                LEFT JOIN GENERIC_DETAIL gd_category ON gd_category.FK_GENERIC_HEADPAR = u.FKGH_HAS_UNIT_CATE
                    AND gd_category.SERIAL_NUM = u.FKGD_HAS_UNIT_CATE AND gd_category.ENTRY_STATUS = '1'
                LEFT JOIN GENERIC_DETAIL gd_services ON gd_services.FK_GENERIC_HEADPAR = 'D104'
                    AND gd_services.SERIAL_NUM = 1 AND gd_services.ENTRY_STATUS = '1'
                LEFT JOIN GENERIC_DETAIL gd_mobile ON gd_mobile.FK_GENERIC_HEADPAR = 'D70'
                    AND gd_mobile.SERIAL_NUM = 1 AND gd_mobile.ENTRY_STATUS = '1'
                LEFT JOIN GENERIC_DETAIL gd_license ON gd_license.FK_GENERIC_HEADPAR = 'LICNS'
                    AND gd_license.SERIAL_NUM = 1 AND gd_license.ENTRY_STATUS = '1'
                LEFT JOIN GENERIC_DETAIL gd_fsr ON gd_fsr.FK_GENERIC_HEADPAR = 'FSRCD'
                    AND gd_fsr.SERIAL_NUM = u.CODE AND gd_fsr.ENTRY_STATUS = '1'
                WHERE u.ENTRY_STATUS = '1' 
                    AND u.CODE IS NOT NULL 
                    AND u.INACTIVE_UNIT = '0'
                    AND u.TMSTAMP >= TIMESTAMP('2016-01-01 00:00:00')
                ORDER BY u.TMSTAMP, u.CODE
                FETCH FIRST 1000 ROWS ONLY
                """,
                timestamp_column='lastModified',
                target_table='branch',
                queue_name='branch_queue',
                processor_class='BranchProcessor',
                batch_size=500,
                poll_interval=30
            ),
            'agents': TableConfig(
                name='agents',
                query="""
                SELECT
                    VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate,
                    TRIM(COALESCE(c.FIRST_NAME, '') || ' ' || COALESCE(c.MIDDLE_NAME, '') || ' ' || COALESCE(c.SURNAME, '')) AS agentName,
                    CAST(c.CUST_ID AS VARCHAR(50)) AS agentId,
                    COALESCE(RIGHT(c.MOBILE_TEL, 6), CAST(c.CUST_ID AS VARCHAR(8))) AS tillNumber,
                    CASE 
                        WHEN c.CUST_TYPE = '1' THEN 'Individual'
                        WHEN c.CUST_TYPE = '2' THEN 'Corporate'
                        WHEN c.CUST_TYPE = 'B' THEN 'Business'
                        ELSE 'Other'
                    END AS businessForm,
                    'ThirdPartyAgent' AS agentPrincipal,
                    TRIM(COALESCE(c.FIRST_NAME, '') || ' ' || COALESCE(c.MIDDLE_NAME, '') || ' ' || COALESCE(c.SURNAME, '')) AS agentPrincipalName,
                    CASE 
                        WHEN c.SEX = 'M' THEN 'Male'
                        WHEN c.SEX = 'F' THEN 'Female'
                        ELSE 'NotSpecified'
                    END AS gender,
                    VARCHAR_FORMAT(COALESCE(c.CUSTOMER_BEGIN_DAT, CURRENT_DATE), 'DDMMYYYYHHMM') AS registrationDate,
                    CASE 
                        WHEN c.ENTRY_STATUS = '0' 
                            THEN VARCHAR_FORMAT(COALESCE(c.LAST_UPDATE, CURRENT_DATE), 'DDMMYYYYHHMM')
                        ELSE NULL
                    END AS closedDate,
                    COALESCE(c.CHAMBER_ID, 'CERT' || CAST(c.CUST_ID AS VARCHAR(10))) AS certIncorporation,
                    'Tanzania' AS nationality,
                    CASE 
                        WHEN c.ENTRY_STATUS = '1' THEN 'Active'
                        WHEN c.ENTRY_STATUS = '0' THEN 'Inactive'
                        ELSE 'Suspended'
                    END AS agentStatus,
                    CASE 
                        WHEN c.CUST_TYPE = '1' THEN 'Individual'
                        WHEN c.CUST_TYPE = '2' THEN 'Corporate'
                        WHEN c.CUST_TYPE = 'B' THEN 'Business'
                        ELSE 'Other'
                    END AS agentType,
                    'ACC' || CAST(c.CUST_ID AS VARCHAR(10)) AS accountNumber,
                    'Dar es Salaam' AS region,
                    'Kinondoni' AS district,
                    'Msasani' AS ward,
                    COALESCE(c.EMPLOYER_ADDRESS, 'Unknown Street') AS street,
                    'Plot 123' AS houseNumber,
                    COALESCE(c.DAI_NUMBER, '12345') AS postalCode,
                    'Tanzania' AS country,
                    '0.0000,0.0000' AS gpsCoordinates,
                    COALESCE(c.CHAMBER_ID, 'TIN' || CAST(c.CUST_ID AS VARCHAR(10))) AS agentTaxIdentificationNumber,
                    COALESCE(c.CHAMBER_ID, 'BL' || CAST(c.CUST_ID AS VARCHAR(10))) AS businessLicense,
                    COALESCE(c.LAST_UPDATE, CURRENT_TIMESTAMP) AS lastModified
                FROM CUSTOMER c
                WHERE c.ENTRY_STATUS = '1'
                    AND c.CUST_TYPE = '2'
                    AND c.MOBILE_TEL IS NOT NULL 
                    AND c.MOBILE_TEL != ''
                    AND LENGTH(TRIM(c.MOBILE_TEL)) > 5
                    AND COALESCE(c.LAST_UPDATE, c.CUSTOMER_BEGIN_DAT) >= TIMESTAMP('2016-01-01 00:00:00')
                ORDER BY COALESCE(c.LAST_UPDATE, c.CUSTOMER_BEGIN_DAT), c.CUST_ID
                FETCH FIRST 1000 ROWS ONLY
                """,
                timestamp_column='lastModified',
                target_table='agents',
                queue_name='agents_queue',
                processor_class='AgentProcessor',
                batch_size=500,
                poll_interval=30
            )
        }
    
    def get_queue_names(self) -> List[str]:
        """Get all queue names"""
        queues = []
        for table_config in self.tables.values():
            queues.extend([
                table_config.queue_name,
                f"{table_config.queue_name}_retry",
                f"{table_config.queue_name}_dlq"
            ])
        return queues