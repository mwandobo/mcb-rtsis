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
                target_table='balancesBot',
                queue_name='balances_bot_queue',
                processor_class='BotBalancesProcessor',
                batch_size=1000,
                poll_interval=10
            ),
            'balanceWithMnos': TableConfig(
                name='balanceWithMnos',
                query="""
                SELECT 
                    latest.reportingDate,
                    latest.floatBalanceDate,
                    latest.mnoCode,
                    latest.tillNumber,
                    latest.currency,
                    latest.allowanceProbableLoss,
                    latest.botProvision,
                    latest.orgFloatAmount,
                    latest.usdFloatAmount,
                    latest.tzsFloatAmount
                FROM (
                    SELECT
                        gte.TRN_DATE AS reportingDate,
                        gte.TRN_DATE AS floatBalanceDate,
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
                        END AS tzsFloatAmount,
                        ROW_NUMBER() OVER (
                            PARTITION BY gl.EXTERNAL_GLACCOUNT 
                            ORDER BY gte.TRN_DATE DESC, gte.DC_AMOUNT DESC
                        ) AS rn
                    FROM GLI_TRX_EXTRACT gte
                    JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
                    WHERE gl.EXTERNAL_GLACCOUNT IN ('504080001','144000051','144000058','144000061','144000062')
                        AND gte.TRN_DATE >= '2024-01-01'
                ) latest
                WHERE latest.rn = 1
                ORDER BY latest.reportingDate DESC
                """,
                timestamp_column='TRN_DATE',
                target_table='balanceWithMnos',
                queue_name='balance_with_mnos_queue',
                processor_class='MnosProcessor',
                batch_size=1000,
                poll_interval=10
            ),
            'balance_with_other_banks': TableConfig(
                name='balance_with_other_banks',
                query="""
                SELECT CURRENT_TIMESTAMP                                  AS reportingDate,
                       pa.ACCOUNT_NUMBER                                  as accountNumber,
                       c.SURNAME                                          as accountName,
                       CASE
                           WHEN UPPER(c.FIRST_NAME) = 'ECOBANK' THEN '040'
                           WHEN UPPER(c.FIRST_NAME) = 'BOA' THEN '009'
                           WHEN UPPER(c.FIRST_NAME) = 'TPB' THEN '048'
                           WHEN UPPER(c.FIRST_NAME) = 'TANZANIA POSTAL BANK' THEN '048'
                           END                                            AS bankCode,
                       'TANZANIA, UNITED REPUBLIC OF'                     as Country,
                       'Domestic bank related'                            as relationshipType,
                       'Current'                                          as accountType,
                       null                                               as subAccountType,
                       gte.CURRENCY_SHORT_DES                             as currency,
                       gte.DC_AMOUNT                                      AS orgAmount,
                       CASE
                           WHEN gte.CURRENCY_SHORT_DES = 'USD'
                               THEN DECIMAL(gte.DC_AMOUNT, 18, 2)
                           WHEN gte.CURRENCY_SHORT_DES IN ('TZ', 'TZS')
                               THEN DECIMAL(gte.DC_AMOUNT / 2500.00, 18, 2)
                           ELSE
                               NULL
                           END                                            AS usdAmount,
                       CASE
                           WHEN gte.CURRENCY_SHORT_DES = 'USD'
                               THEN DECIMAL(gte.DC_AMOUNT * 2500.00, 18, 2)
                           WHEN gte.CURRENCY_SHORT_DES IN ('TZ', 'TZS')
                               THEN DECIMAL(gte.DC_AMOUNT, 18, 2)
                           ELSE
                               NULL
                           END                                            AS tzsAmount,
                       gte.TRN_DATE                                       as transactionDate,
                       (DATE(gte.AVAILABILITY_DATE) - DATE(gte.TRN_DATE)) AS pastDueDays,
                       0                                                  as allowanceProbableLoss,
                       0                                                  as botProvision,
                       'Current'                                          as assetsClassificationCategory,
                       gte.TRN_DATE                                       as contractDate,
                       gte.AVAILABILITY_DATE                              as maturityDate,
                       'Highly rated Multilateral Development Banks'      as externalRatingCorrespondentBank,
                       NULL                                               as gradesUnratedBanks
                FROM GLI_TRX_EXTRACT as gte
                         LEFT JOIN
                     GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
                         LEFT JOIN
                     CUSTOMER c ON gte.CUST_ID = c.CUST_ID
                         LEFT JOIN
                     PROFITS.PROFITS_ACCOUNT pa ON gte.CUST_ID = pa.CUST_ID and PRFT_SYSTEM = 3
                where gl.EXTERNAL_GLACCOUNT IN ('100050001','100013000','100050000') AND pa.ACCOUNT_NUMBER <> ''
                ORDER BY gte.TRN_DATE
                FETCH FIRST 1000 ROWS ONLY
                """,
                timestamp_column='TRN_DATE',
                target_table='balanceWithOtherBank',
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
                SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHH24MI')                      AS reportingDate,
                       LTRIM(RTRIM(pa.ACCOUNT_NUMBER))                                          AS accountNumber,
                       LTRIM(RTRIM(id.ID_NO))                                                   AS customerIdentificationNumber,
                       wela.CUSTOMER_NAME                                                       AS clientName,
                       ctl.CUSTOMER_TYPE                                                        as clientType,
                       cl.COUNTRY_CODE                                                          as borrowerCountry,
                       null                                                                     as ratingStatus,
                       null                                                                     as crRatingBorrower,
                       null                                                                     as gradesUnratedBanks,
                       null                                                                     as groupCode,
                       null                                                                     as relatedEntityName,
                       null                                                                     as relatedParty,
                       null                                                                     as relationshipCategory,
                       wela.LOAN_TYPE_NAME                                                      as loanProductType,
                       'OtherServices'                                                          as overdraftEconomicActivity,
                       'Existing'                                                               as loanPhase,
                       'NotSpecified'                                                           as transferStatus,
                       CASE wela.PRODUCT_DESC
                           WHEN 'STAFF PERSONAL LOANS' THEN 'STAFF LOAN'
                           WHEN 'OVERDRAFT BUSINESS' THEN 'BUSINESS OVERDRAFT'
                           WHEN 'TRADE FINANCE' THEN 'TRADE FINANCE'
                           ELSE 'Other'
                           END                                                                  AS purposeOtherLoans,
                       wela.ACC_OPEN_DT                                                         as contractDate,
                       wela.FK_UNITCODE                                                         as branchCode,
                       wela.LOAN_OFFICER_NAME                                                   as loanOfficer,
                       null                                                                     as loanSupervisor,
                       wela.CURRENCY                                                            as currency,
                       wela.ACC_LIMIT_AMN                                                       as orgSanctionedAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD'
                               THEN wela.ACC_LIMIT_AMN
                           ELSE NULL
                           END                                                                  AS usdSanctionedAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD'
                               THEN wela.ACC_LIMIT_AMN * 2500
                           ELSE
                               wela.ACC_LIMIT_AMN
                           END                                                                  AS tzsSanctionedAmount,
                       wela.TOT_DRAWDOWN_AMN                                                    as orgUtilisedAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD'
                               THEN wela.TOT_DRAWDOWN_AMN
                           ELSE NULL
                           END                                                                  AS usdUtilisedAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD'
                               THEN wela.TOT_DRAWDOWN_AMN * 2500
                           ELSE
                               wela.TOT_DRAWDOWN_AMN
                           END                                                                  AS tzsUtilisedAmount,
                       wela.TRX_AMN                                                             as orgCrUsageLast30DaysAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD'
                               THEN wela.TRX_AMN
                           ELSE NULL
                           END                                                                  AS usdCrUsageLast30DaysAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD'
                               THEN wela.TRX_AMN * 2500
                           ELSE
                               wela.TRX_AMN
                           END                                                                  AS tzsCrUsageLast30DaysAmount,
                       wela.DRAWDOWN_FST_DT                                                     AS disbursementDate,
                       wela.ACC_EXP_DT                                                          AS expiryDate,
                       COALESCE(wela.WRITE_OFF_DATE, wela.OV_EXP_DT, wela.ACC_EXP_DT)           AS realEndDate,
                       (wela.NRM_BALANCE + wela.OV_BALANCE)                                     AS orgOutstandingAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD' THEN (wela.NRM_BALANCE + wela.OV_BALANCE)
                           ELSE NULL
                           END                                                                  AS usdOutstandingAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD' THEN (wela.NRM_BALANCE + wela.OV_BALANCE) * 2500
                           ELSE (wela.NRM_BALANCE + wela.OV_BALANCE)
                           END                                                                  AS tzsOutstandingAmount,
                       wela.DRAWDOWN_FST_DT                                                     AS latestCustomerCreditDate,
                       wela.DRAWDOWN_FST_AMN                                                    AS latestCreditAmount,
                       wela.SELECTED_BANK_RATE                                                  AS primeLendingRate,
                       wela.FINAL_INTEREST                                                      AS annualInterestRate,
                       wela.COLLATERAL_OM_VALUE                                                 AS collateralPledged,
                       wela.COLLATERAL_OM_VALUE                                                 AS orgCollateralValue,
                       CASE
                           WHEN wela.CURRENCY = 'USD' THEN wela.COLLATERAL_OM_VALUE
                           ELSE NULL
                           END                                                                  AS usdCollateralValue,
                       CASE
                           WHEN wela.CURRENCY = 'USD' THEN wela.COLLATERAL_OM_VALUE * 2500
                           ELSE wela.COLLATERAL_OM_VALUE
                           END                                                                  AS tzsCollateralValue,
                       CASE WHEN wela.ACC_DRAWDOWN_STS = 'R' THEN 1 ELSE 0 END                  AS restructuredLoans,
                       wela.OVERDUE_DAYS                                                        AS pastDueDays,
                       wela.OV_BALANCE                                                          AS pastDueAmount,
                       wela.NRM_ACR_INT_BAL + wela.OV_ACR_NRM_INT_BAL + wela.OV_ACR_PNL_INT_BAL AS orgAccruedInterestAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD' THEN wela.NRM_ACR_INT_BAL + wela.OV_ACR_NRM_INT_BAL + wela.OV_ACR_PNL_INT_BAL
                           ELSE NULL
                           END                                                                  AS usdAccruedInterestAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD' THEN (wela.NRM_ACR_INT_BAL + wela.OV_ACR_NRM_INT_BAL + wela.OV_ACR_PNL_INT_BAL) * 2500
                           ELSE wela.NRM_ACR_INT_BAL + wela.OV_ACR_NRM_INT_BAL + wela.OV_ACR_PNL_INT_BAL
                           END                                                                  AS tzsAccruedInterestAmount,
                       wela.OV_RL_PNL_INT_BAL + wela.OV_URL_PNL_INT_BAL                         AS orgPenaltyChargedAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD' THEN wela.OV_RL_PNL_INT_BAL + wela.OV_URL_PNL_INT_BAL
                           ELSE NULL
                           END                                                                  AS usdPenaltyChargedAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD' THEN (wela.OV_RL_PNL_INT_BAL + wela.OV_URL_PNL_INT_BAL) * 2500
                           ELSE wela.OV_RL_PNL_INT_BAL + wela.OV_URL_PNL_INT_BAL
                           END                                                                  AS tzsPenaltyChargedAmount,
                       wela.TOT_COMMISSION_AMN                                                  AS orgLoanFeesChargedAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD' THEN wela.TOT_COMMISSION_AMN
                           ELSE NULL
                           END                                                                  AS usdLoanFeesChargedAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD' THEN wela.TOT_COMMISSION_AMN * 2500
                           ELSE wela.TOT_COMMISSION_AMN
                           END                                                                  AS tzsLoanFeesChargedAmount,
                       wela.TOT_EXPENSE_AMN                                                     AS orgLoanFeesPaidAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD' THEN wela.TOT_EXPENSE_AMN
                           ELSE NULL
                           END                                                                  AS usdLoanFeesPaidAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD' THEN wela.TOT_EXPENSE_AMN * 2500
                           ELSE wela.TOT_EXPENSE_AMN
                           END                                                                  AS tzsLoanFeesPaidAmount,
                       wela.INSTALL_FIXED_AMN                                                   AS orgTotMonthlyPaymentAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD' THEN wela.INSTALL_FIXED_AMN
                           ELSE NULL
                           END                                                                  AS usdTotMonthlyPaymentAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD' THEN wela.INSTALL_FIXED_AMN * 2500
                           ELSE wela.INSTALL_FIXED_AMN
                           END                                                                  AS tzsTotMonthlyPaymentAmount,
                       wela.TOT_NRM_INT_AMN + wela.TOT_PNL_INT_AMN                              AS orgInterestPaidTotal,
                       CASE
                           WHEN wela.CURRENCY = 'USD' THEN wela.TOT_NRM_INT_AMN + wela.TOT_PNL_INT_AMN
                           ELSE NULL
                           END                                                                  AS usdInterestPaidTotal,
                       CASE
                           WHEN wela.CURRENCY = 'USD' THEN (wela.TOT_NRM_INT_AMN + wela.TOT_PNL_INT_AMN) * 2500
                           ELSE wela.TOT_NRM_INT_AMN + wela.TOT_PNL_INT_AMN
                           END                                                                  AS tzsInterestPaidTotal,
                       'Current'                                                                AS assetClassificationCategory,
                       wela.CLOAN_CATEGORY_DESCRIPTION                                          AS sectorSnaClassification,
                       wela.ACC_STATUS                                                          AS negStatusContract,
                       wela.CUST_TYPE                                                           AS customerRole,
                       wela.PROVISION_AMOUNT                                                    AS allowanceProbableLoss,
                       wela.PROVISION_AMN                                                       AS botProvision
                FROM W_EOM_LOAN_ACCOUNT wela
                         LEFT JOIN CUSTOMER c ON wela.CUST_ID = c.CUST_ID
                         LEFT JOIN PROFITS_ACCOUNT pa ON pa.CUST_ID = wela.CUST_ID
                         LEFT JOIN PRODUCT p ON p.ID_PRODUCT = wela.ID_PRODUCT
                         LEFT JOIN other_id id ON (CASE WHEN (id.serial_no IS NULL) THEN '1' ELSE id.main_flag END = '1' AND
                                                   id.fk_customercust_id = c.cust_id)
                         LEFT JOIN cust_address ca ON (ca.fk_customercust_id = c.cust_id AND ca.communication_addr = '1' AND
                                                       ca.entry_status = '1')
                         LEFT JOIN generic_detail id_country ON (id.fkgh_has_been_issu = id_country.fk_generic_headpar AND
                                                                 id.fkgd_has_been_issu = id_country.serial_num)
                         LEFT JOIN CUSTOMER_TYPES_LOOKUP ctl ON ctl.CUSTOMER_TYPE_CODE = c.CUST_TYPE
                         LEFT JOIN COUNTRIES_LOOKUP cl ON cl.COUNTRY_NAME = id_country.description
                WHERE wela.EOM_DATE >= CURRENT DATE - 300 DAYS
                  and wela.OVERDRAFT_TYPE_FLAG = 'Overdraft'
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
                SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate,
                       COALESCE(
                               u.UNIT_NAME,
                               u.UNIT_NAME_LATIN,
                               'Unknown Branch'
                       )                                                 AS branchName,
                       117039447                                         AS taxIdentificationNumber,
                       CASE
                           WHEN u.CODE = 201 THEN 'BL20000102884'
                           WHEN u.CODE = 200 THEN 'BL20000102884'
                           END                                           AS businessLicense,
                       CAST(u.CODE AS VARCHAR(10))                       AS branchCode,
                       'FSR-' || CAST(u.CODE AS VARCHAR(10))    AS qrFsrCode,
                       U.ADDRESS_2                                       AS region,
                       CASE
                           WHEN u.CODE = 201 THEN 'Ubungo'
                           WHEN u.CODE = 200 THEN 'Ilala'
                           END                                           AS district,
                       CASE
                           WHEN u.CODE = 201 THEN 'Sinza'
                           WHEN u.CODE = 200 THEN 'Kisutu'
                           END                                           AS ward,
                       CASE
                           WHEN u.CODE = 201 THEN 'Sam Nujoma Road'
                           WHEN u.CODE = 200 THEN 'Samora Avenue'
                           END                                           AS street,
                       null                                              AS houseNumber,
                       u.ADDRESS_LATIN                                   AS postalCode,
                       CASE
                           WHEN u.LATITUDE_LOCATION IS NOT NULL AND u.LONGITUDE_LOCATION IS NOT NULL
                               THEN TRIM(u.LATITUDE_LOCATION) || ',' || TRIM(u.LONGITUDE_LOCATION)
                           WHEN u.GEO_AREA IS NOT NULL
                               THEN u.GEO_AREA
                           ELSE '0.0000,0.0000'
                           END                                           AS gpsCoordinates,
                       CASE
                           WHEN u.CODE = 201 OR U.CODE = 200 THEN 'Fully fledged'
                           ELSE 'Service center'
                           END                                           AS bankingServices,
                       'M-Pesa Airtel Money Tigo Pesa Halopesa'          AS mobileMoneyServices,
                       VARCHAR_FORMAT(
                               DATE(u.TMSTAMP),
                               'DDMMYYYYHHMM'
                       )                                                 AS registrationDate,
                       CASE
                           WHEN u.ENTRY_STATUS = '1' AND u.INACTIVE_UNIT != '0' THEN 'Active'
                           WHEN u.ENTRY_STATUS = '1' AND u.INACTIVE_UNIT = '0' THEN 'Inactive'
                           WHEN u.ENTRY_STATUS = '0' THEN 'Closed'
                           ELSE 'Unknown'
                           END                                           AS branchStatus,
                       null                                              AS closureDate,
                       CASE
                           WHEN u.CODE = 201 THEN 'Furahini S. Lema'
                           WHEN u.CODE = 200 THEN 'Monica G. Malisa'
                           END                                           AS contactPerson,
                       CASE
                           WHEN u.CODE = 201 THEN '0713249528'
                           WHEN u.CODE = 200 THEN '0682697276'
                           END                                           AS telephoneNumber,
                       CASE
                           WHEN u.CODE = 201 THEN '0756818609'
                           WHEN u.CODE = 200 THEN '0682697276'
                           END                                           AS altTelephoneNumber,
                       'Brick and Mortar'                                AS branchCategory,
                       u.TMSTAMP AS lastModified
                FROM UNIT u WHERE
                    (u.UNIT_NAME = 'MLIMANI BRANCH' OR u.UNIT_NAME = 'SAMORA BRANCH')
                    AND u.TMSTAMP >= TIMESTAMP('2016-01-01 00:00:00')
                ORDER BY u.CODE
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
                SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')                                            AS reportingDate,
                       TRIM(
                               CAST(TRIM(COALESCE(be.FIRST_NAME, '')) AS VARCHAR(100)) ||
                               CASE
                                   WHEN TRIM(COALESCE(be.FATHER_NAME, '')) <> ''
                                       THEN ' ' || CAST(TRIM(be.FATHER_NAME) AS VARCHAR(100))
                                   ELSE ''
                                   END ||
                               CASE
                                   WHEN TRIM(COALESCE(be.LAST_NAME, '')) <> ''
                                       THEN ' ' || CAST(TRIM(be.LAST_NAME) AS VARCHAR(100))
                                   ELSE ''
                                   END
                       )                                                                                            AS agentName,
                       al.AGENT_ID                                                                                  AS agentId,
                       null                                                                                         AS tillNumber,
                       CASE
                           WHEN UPPER(TRIM(al.BUSINESS_FORM)) = 'SOLE PROPRIETORY' THEN 'Sole Proprietor'
                           WHEN UPPER(TRIM(al.BUSINESS_FORM)) = 'LIMITED COMPANY' THEN 'Company'
                           WHEN UPPER(TRIM(al.BUSINESS_FORM)) = 'PRIVATE COMPANY' THEN 'Company'
                           WHEN UPPER(TRIM(al.BUSINESS_FORM)) = 'CO-OPERATIVE SOCIETY' THEN 'Trust'
                           WHEN UPPER(TRIM(al.BUSINESS_FORM)) = 'PARTNERSHIP' THEN 'Partnership'
                           ELSE TRIM(al.BUSINESS_FORM)
                           END                                                                                      AS businessForm,
                       'bank'                                                                                       AS agentPrincipal,
                       'Selcom'                                                                                     AS agentPrincipalName,
                       CASE WHEN be.SEX = 'M' then 'Male' WHEN be.SEX = 'F' then 'female' ELSE 'Not Applicable' END AS gender,
                       VARCHAR_FORMAT(COALESCE(be.TMSTAMP, CURRENT_DATE), 'DDMMYYYYHHMM')                           AS registrationDate,
                       null                                                                                         AS closedDate,
                       al.CERT_IN_CORPORATION                                                                       AS certIncorporation,
                       'TANZANIA, UNITED REPUBLIC OF'                                                               AS nationality,
                       CASE
                           WHEN be.EMPL_STATUS = '1' THEN 'Active'
                           WHEN be.EMPL_STATUS = '0' THEN 'Inactive'
                           ELSE 'Suspended'
                           END                                                                                      AS agentStatus,
                       'super agent'                                                                                AS agentType,
                       null                                                                                         AS accountNumber,
                       COALESCE(region_lkp.BOT_REGION, al.REGION)                                                   AS region,
                       COALESCE(district_lkp.BOT_DISTRICT, al.DISTRICT)                                             AS district,
                       COALESCE(ward_lkp.BOT_WARD, al.LOCATION, 'N/A')                                              AS ward,
                       'N/A'                                                                                        AS street,
                       'N/A'                                                                                        AS houseNumber,
                       'N/A'                                                                                        AS postalCode,
                       'TANZANIA, UNITED REPUBLIC OF'                                                               AS country,
                       al.GPS                                                                                       AS gpsCoordinates,
                       al.TIN                                                                                       AS agentTaxIdentificationNumber,
                       CASE
                           -- 1️⃣ Comma exists
                           WHEN LOCATE(',', al.BUSINESS_LICENCE_ISSUER_AND_DATE) > 0 THEN
                               CASE
                                   -- 1a. If there's a space before the comma, use first word (up to first space)
                                   WHEN LOCATE(' ', SUBSTR(al.BUSINESS_LICENCE_ISSUER_AND_DATE, 1,
                                                           LOCATE(',', al.BUSINESS_LICENCE_ISSUER_AND_DATE) - 1)) > 0 THEN
                                       TRIM(
                                               SUBSTR(
                                                       al.BUSINESS_LICENCE_ISSUER_AND_DATE,
                                                       1,
                                                       LOCATE(' ', al.BUSINESS_LICENCE_ISSUER_AND_DATE) - 1
                                               )
                                       )
                                   -- 1b. No space before comma, use everything before comma
                                   ELSE
                                       TRIM(
                                               SUBSTR(
                                                       al.BUSINESS_LICENCE_ISSUER_AND_DATE,
                                                       1,
                                                       LOCATE(',', al.BUSINESS_LICENCE_ISSUER_AND_DATE) - 1
                                               )
                                       )
                                   END
                           -- 2️⃣ No comma, but space exists
                           WHEN LOCATE(' ', al.BUSINESS_LICENCE_ISSUER_AND_DATE) > 0 THEN
                               TRIM(
                                       SUBSTR(
                                               al.BUSINESS_LICENCE_ISSUER_AND_DATE,
                                               1,
                                               LOCATE(' ', al.BUSINESS_LICENCE_ISSUER_AND_DATE) - 1
                                       )
                               )
                           -- 3️⃣ Last resort: use whole string
                           ELSE TRIM(al.BUSINESS_LICENCE_ISSUER_AND_DATE)
                           END                                                                                      AS businessLicense,
                       COALESCE(be.TMSTAMP, CURRENT_TIMESTAMP)                                                      AS lastModified
                FROM AGENTS_LIST al
                         RIGHT JOIN BANKEMPLOYEE be
                                    ON RIGHT(TRIM(al.TERMINAL_ID), 8) = TRIM(be.STAFF_NO)
                         LEFT JOIN (SELECT al.AGENT_ID,
                                           bl.REGION AS BOT_REGION,
                                           ROW_NUMBER() OVER (
                                               PARTITION BY al.AGENT_ID
                                               ORDER BY
                                                   CASE
                                                       WHEN UPPER(TRIM(al.REGION)) = UPPER(TRIM(bl.REGION)) THEN 1 -- exact
                                                       WHEN UPPER(TRIM(al.REGION)) LIKE UPPER(TRIM(bl.REGION)) || '%'
                                                           THEN 2 -- safe starts-with
                                                       ELSE 99 -- do not allow random fallback
                                                       END,
                                                   LENGTH(TRIM(bl.REGION)) DESC
                                               )     AS rn
                                    FROM AGENTS_LIST al
                                             JOIN BANK_LOCATION_LOOKUP_V2 bl
                                                  ON UPPER(TRIM(al.REGION)) = UPPER(TRIM(bl.REGION))
                                                      OR (UPPER(TRIM(al.REGION)) LIKE UPPER(TRIM(bl.REGION)) || '%' AND
                                                          LENGTH(TRIM(bl.REGION)) >= 4)) region_lkp
                                   ON region_lkp.AGENT_ID = al.AGENT_ID
                                       AND region_lkp.rn = 1

                         LEFT JOIN (SELECT al.AGENT_ID,
                                           bl.DISTRICT AS BOT_DISTRICT,
                                           ROW_NUMBER() OVER (
                                               PARTITION BY al.AGENT_ID
                                               ORDER BY
                                                   CASE
                                                       -- 1️⃣ Exact match
                                                       WHEN UPPER(TRIM(al.DISTRICT)) = UPPER(TRIM(bl.DISTRICT)) THEN 1

                                                       -- 2️⃣ Starts-with match (safe)
                                                       WHEN UPPER(TRIM(al.DISTRICT)) LIKE UPPER(TRIM(bl.DISTRICT)) || '%'
                                                           AND LENGTH(TRIM(bl.DISTRICT)) >= 4 THEN 2

                                                       -- 3️⃣ No fallback
                                                       ELSE 99
                                                       END,
                                                   LENGTH(TRIM(bl.DISTRICT)) DESC
                                               )       AS rn
                                    FROM AGENTS_LIST al
                                             JOIN BANK_LOCATION_LOOKUP_V2 bl
                                                  ON (
                                                      UPPER(TRIM(al.DISTRICT)) = UPPER(TRIM(bl.DISTRICT))
                                                          OR (
                                                          UPPER(TRIM(al.DISTRICT)) LIKE UPPER(TRIM(bl.DISTRICT)) || '%'
                                                              AND LENGTH(TRIM(bl.DISTRICT)) >= 4
                                                          )
                                                      )
                                    WHERE TRIM(al.DISTRICT) IS NOT NULL
                                      AND TRIM(al.DISTRICT) <> '') district_lkp
                                   ON district_lkp.AGENT_ID = al.AGENT_ID
                                       AND district_lkp.rn = 1
                         LEFT JOIN (SELECT al.AGENT_ID,
                                           bl.WARD AS BOT_WARD,
                                           ROW_NUMBER() OVER (
                                               PARTITION BY al.AGENT_ID
                                               ORDER BY
                                                   CASE
                                                       -- 1️⃣ Exact match
                                                       WHEN UPPER(TRIM(al.LOCATION)) = UPPER(TRIM(bl.WARD)) THEN 1

                                                       -- 2️⃣ Starts-with match (safe)
                                                       WHEN UPPER(TRIM(al.LOCATION)) LIKE UPPER(TRIM(bl.WARD)) || '%'
                                                           AND LENGTH(TRIM(bl.WARD)) >= 4 THEN 2

                                                       -- 3️⃣ No fallback
                                                       ELSE 99
                                                       END,
                                                   LENGTH(TRIM(bl.WARD)) DESC
                                               )   AS rn
                                    FROM AGENTS_LIST al
                                             JOIN BANK_LOCATION_LOOKUP_V2 bl
                                                  ON (
                                                      UPPER(TRIM(al.LOCATION)) = UPPER(TRIM(bl.WARD))
                                                          OR (
                                                          UPPER(TRIM(al.LOCATION)) LIKE UPPER(TRIM(bl.WARD)) || '%'
                                                              AND LENGTH(TRIM(bl.WARD)) >= 4
                                                          )
                                                      )
                                    WHERE TRIM(al.LOCATION) IS NOT NULL
                                      AND TRIM(al.LOCATION) <> '') ward_lkp
                                   ON ward_lkp.AGENT_ID = al.AGENT_ID
                                       AND ward_lkp.rn = 1

                WHERE be.STAFF_NO IS NOT NULL
                  AND be.STAFF_NO = TRIM(be.STAFF_NO)
                  AND be.EMPL_STATUS = 1
                  AND be.STAFF_NO NOT LIKE 'ATMUSER%'
                  AND be.STAFF_NO NOT LIKE '993%'
                  AND be.STAFF_NO NOT LIKE '999%'
                  AND be.STAFF_NO NOT LIKE '900%'
                  AND be.STAFF_NO NOT LIKE 'IAP%'
                  AND be.STAFF_NO NOT LIKE 'MCB%'
                  AND be.STAFF_NO NOT LIKE 'MIP%'
                  AND be.STAFF_NO NOT LIKE 'MOB%'
                  AND be.STAFF_NO NOT LIKE 'MWL%'
                  AND be.STAFF_NO NOT LIKE 'OWP%'
                  AND be.STAFF_NO NOT LIKE 'PI0%'
                  AND be.STAFF_NO NOT LIKE 'POS%'
                  AND be.STAFF_NO NOT LIKE 'STP%'
                  AND be.STAFF_NO NOT LIKE 'TER%'
                  AND be.STAFF_NO NOT LIKE 'EIC%'
                  AND be.STAFF_NO NOT LIKE 'GEP%'
                  AND be.STAFF_NO NOT LIKE 'EYU%'
                  AND be.STAFF_NO NOT LIKE 'GLA%'
                  AND be.STAFF_NO NOT LIKE 'SYS%'
                  AND be.STAFF_NO NOT LIKE 'MLN%'
                  AND be.STAFF_NO NOT LIKE 'PET%'
                  AND be.STAFF_NO NOT LIKE 'VRT%'
                ORDER BY be.TMSTAMP, al.AGENT_ID
                FETCH FIRST 1000 ROWS ONLY
                """,
                timestamp_column='lastModified',
                target_table='agents',
                queue_name='agents_queue',
                processor_class='AgentProcessor',
                batch_size=500,
                poll_interval=30
            ),
            'atm_information': TableConfig(
                name='atm_information',
                query="""
                SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')                    AS reportingDate,
                       be.FIRST_NAME                                                        AS atmName,
                       b.branchCode,
                       be.STAFF_NO                                                          AS atmCode,
                       CASE WHEN b.branchCode = 200 THEN '200' ELSE '300' END               AS tillNumber,
                       'M-Pesa'                                                             AS mobileMoneyServices,
                       'FSR-' || CAST(be.STAFF_NO AS VARCHAR(10))                           AS qrFsrCode,
                       NULL                                                                 AS postalCode,
                       'DAR ES SALAAM'                                                      AS region,
                       CASE WHEN b.branchCode = 200 THEN 'ILALA' ELSE 'UBUNGO' END          AS district,
                       CASE WHEN b.branchCode = 200 THEN 'KISUTU' ELSE 'UBUNGO WARD' END    AS ward,
                       CASE WHEN b.branchCode = 200 THEN 'SAMORA STREET' ELSE 'MLIMANI' END AS street,
                       NULL                                                                 AS houseNumber,
                       CASE
                           WHEN u.LATITUDE_LOCATION IS NOT NULL AND u.LONGITUDE_LOCATION IS NOT NULL
                               THEN TRIM(u.LATITUDE_LOCATION) || ',' || TRIM(u.LONGITUDE_LOCATION)
                           WHEN u.GEO_AREA IS NOT NULL
                               THEN u.GEO_AREA
                           ELSE '0.0000,0.0000'
                           END                                                              AS gpsCoordinates,
                       CASE WHEN b.branchCode = 200 THEN '101000010' ELSE '101000015' END   AS linkedAccount,
                       VARCHAR_FORMAT(be.TMSTAMP, 'DDMMYYYYHHMM')                           AS openingDate,
                       'active'                                                             AS atmStatus,
                       null                                                                 AS closureDate,
                       'onsite'                                                             AS atmCategory,
                       'Card and Mobile Based'                                              AS atmChannel
                FROM BANKEMPLOYEE be
                         JOIN (SELECT STAFF_NO,
                                      CASE
                                          WHEN STAFF_NO = 'MWL01001' THEN 200
                                          ELSE 201
                                          END AS branchCode
                               FROM BANKEMPLOYEE) b
                              ON b.STAFF_NO = be.STAFF_NO
                         JOIN UNIT u
                              ON u.CODE = b.branchCode
                WHERE be.STAFF_NO IS NOT NULL
                  AND be.STAFF_NO = TRIM(be.STAFF_NO)
                  AND be.EMPL_STATUS = 1
                  AND be.STAFF_NO LIKE 'MWL01%'
                  AND u.CODE IN (200, 201)
                ORDER BY be.STAFF_NO ASC
                FETCH FIRST 1000 ROWS ONLY
                """,
                timestamp_column='openingDate',
                target_table='atm_information',
                queue_name='atm_information_queue',
                processor_class='AtmProcessor',
                batch_size=1000,
                poll_interval=10
            ),
            'card_information': TableConfig(
                name='card_information',
                query="""
                SELECT
                    CURRENT_TIMESTAMP AS reportingDate,
                    ca.TUN_UNIT AS bankCode,
                    CA.FULL_CARD_NO AS cardNumber,
                    RIGHT(LPAD(TRIM(CHAR(CA.CARD_NUMBER)), 10, '0'),10) AS binNumber,
                    CA.FK_CUST_ID AS customerIdentificationNumber,
                    'Debit' AS cardType,  
                    NULL AS cardTypeSubCategory,  
                    CA.TUN_DATE AS cardIssueDate, 
                    'Mwalimu Commercial Bank' AS cardIssuer,  
                    'Domestic' AS cardIssuerCategory,  
                    'Tanzania' AS cardIssuerCountry,  
                    CA.CARD_NAME_LATIN AS cardHolderName,  
                    CASE
                        WHEN CURRENT_DATE > CA.CARD_EXPIRY_DATE then 'Active'
                        ELSE 'Inactive'
                    END AS cardStatus, 
                    'VISA' AS cardScheme,  
                    'UBX Tanzania Limited' AS acquiringPartner,  
                    CA.CARD_EXPIRY_DATE AS cardExpireDate  
                FROM CMS_CARD CA
                WHERE CA.TUN_DATE >= DATE('2016-01-01')
                ORDER BY CA.TUN_DATE ASC, CA.FK_CUST_ID ASC
                FETCH FIRST 1000 ROWS ONLY
                """,
                timestamp_column='cardIssueDate',
                target_table='card_information',
                queue_name='card_information_queue',
                processor_class='CardProcessor',
                batch_size=1000,
                poll_interval=10
            ),
            'pos_information': TableConfig(
                name='pos_information',
                query="""
                SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')    AS reportingDate,
                       201                                                  AS posBranchCode,
                       at.FK_USRCODE                                        AS posNumber,
                       'FSR-' || CAST(at.FK_USRCODE AS VARCHAR(10))         AS qrFsrCode,
                       'Selcom Paytech Ltd'                                 AS posHolderName,
                       NULL                                                 AS posHolderNin,
                       '103847451'                                          AS posHolderTin,
                       NULL                                                 AS postalCode,
                       COALESCE(
                               dl.REGION,
                               (SELECT r.REGION
                                FROM PROFITS.BANK_LOCATION_LOOKUP r
                                WHERE UPPER(TRIM(at.LOCATION)) LIKE '%' || UPPER(r.REGION) || '%'
                                    FETCH FIRST 1 ROW ONLY),
                               'DAR ES SALAAM' -- final hardcoded fallback
                       )                                                    AS region,
                       COALESCE(
                               dl.DISTRICT,
                               (SELECT r.DISTRICT
                                FROM PROFITS.BANK_LOCATION_LOOKUP r
                                WHERE UPPER(TRIM(at.LOCATION)) LIKE '%' || UPPER(r.REGION) || '%'
                                    FETCH FIRST 1 ROW ONLY),
                               'ILALA' -- final hardcoded fallback
                       )                                                    AS district,
                       NULL                                                 AS ward,
                       NULL                                                 AS street,
                       NULL                                                 AS houseNumber,
                       NULL                                                 AS gpsCoordinates,
                       '230000070'                                          AS linkedAccount,
                       VARCHAR_FORMAT(at.INSERTION_TMSTAMP, 'DDMMYYYYHHMM') AS issueDate,
                       NULL                                                 AS returnDate
                FROM PROFITS.AGENT_TERMINAL at
                         LEFT JOIN PROFITS.BANK_LOCATION_LOOKUP dl
                                   ON UPPER(TRIM(at.LOCATION)) LIKE '%' || UPPER(dl.DISTRICT) || '%'
                ORDER BY at.FK_USRCODE ASC
                FETCH FIRST 1000 ROWS ONLY
                """,
                timestamp_column='issueDate',
                target_table='posInformation',
                queue_name='pos_information_queue',
                processor_class='POSProcessor',
                batch_size=1000,
                poll_interval=10
            ),
            'card_transaction': TableConfig(
                name='card_transaction',
                query="""
                SELECT
                    CURRENT_TIMESTAMP AS reportingDate,
                    ca.FULL_CARD_NO as cardNumber,
                    LPAD(CHAR(ca.CARD_NUMBER), 10, '0') AS binNumber,
                    'Mwalimu Commercial Bank' as transactingBankName,
                    ce.ISO_REF_NUM as transactionId,
                    ce.TUN_DATE as transactionDate,
                    CASE
                        WHEN PROCESS_CD IN ('001000','002000','011000','011096','012000') THEN 'Cash Withdrawal'
                        WHEN PROCESS_CD = '219610' THEN 'Purchase / Payment'
                        WHEN PROCESS_CD IN ('311000','312000','381000','382000') THEN 'Non-Financial'
                        ELSE 'Other Financial Transaction'
                    END as transactionNature,
                    null as atmCode,
                    null as posNumber,
                    pc.DESCRIPTION as transactionDescription,
                    ca.CARD_NAME_LATIN as beneficiaryName,
                    null as beneficiaryTradeName,
                    'Tanzania' as beneficiaryCountry,
                    'Tanzania' as transactionPlace,
                    null as qtyItemsPurchased,
                    null as unitPrice,
                    null as orgFacilitatorCommissionAmount,
                    null as usdFacilitatorCommissionAmount,
                    null as tzsFacilitatorCommissionAmount,
                    'TZS' as currency,
                    ce.TRANSACTION_AMNT as orgTransactionAmount,
                    CAST(ROUND(ce.TRANSACTION_AMNT / 2500.0, 2) AS DECIMAL(15,2)) AS usdTransactionAmount,
                    ce.TRANSACTION_AMNT as tzsTransactionAmount
                FROM CMS_CARD_EXTRAIT ce
                LEFT JOIN CMS_CARD ca ON ca.CARD_SN = ce.CARD_SN
                LEFT JOIN ATM_PROCESS_CODE pc ON pc.ISO_CODE = ce.PROCESS_CD
                ORDER BY ce.TUN_DATE ASC, ce.ISO_REF_NUM ASC
                FETCH FIRST 1000 ROWS ONLY
                """,
                timestamp_column='transactionDate',
                target_table='cardTransaction',
                queue_name='card_transaction_queue',
                processor_class='CardTransactionProcessor',
                batch_size=1000,
                poll_interval=10
            ),
            'personal_data_information': TableConfig(
                name='personal_data_information',
                query="""
                WITH w_address AS (
                    SELECT *
                    FROM   cust_address c
                    WHERE  (c.fk_customercust_id, c.serial_num) IN (
                        SELECT fk_customercust_id,
                               MIN(serial_num)
                        FROM   cust_address
                        WHERE  address_type = '4'
                          AND  entry_status = '1'
                        GROUP BY fk_customercust_id
                    )
                )
                SELECT
                    CURRENT_TIMESTAMP AS reportingDate,
                    c.cust_id AS customerIdentificationNumber,
                    c.first_name AS firstName,
                    c.middle_name AS middleNames,
                    NULL AS otherNames,
                    TRIM(
                        CASE
                            WHEN c.cust_type = '1' THEN
                                TRIM(NVL(c.first_name, ' ')) || ' ' ||
                                TRIM(NVL(c.middle_name, ' ')) || ' ' ||
                                TRIM(NVL(c.surname, ' '))
                            WHEN c.cust_type = '2' THEN
                                TRIM(c.surname)
                            ELSE ''
                        END
                    ) AS fullNames,
                    c.surname AS presentSurname,
                    c.mother_surname AS birthSurname,
                    c.sex AS gender,
                    NULL AS maritalStatus,
                    NULL AS numberSpouse,
                    c.spouse_name AS spousesFullName,
                    gd_natio.description AS nationality,
                    gd_citiz.description AS citizenship,
                    CASE WHEN c.non_resident = '0' THEN 'Resident' ELSE 'Non-Resident' END AS residency,
                    gd_proff.description AS profession,
                    gd_ccode.description AS sectorSnaClassification,
                    NULL AS fateStatus,
                    NULL AS socialStatus,
                    gd_profcat.description AS employmentStatus,
                    c.salary_amn AS monthlyIncome,
                    c.num_of_children + c.children_above18 AS numberDependants,
                    gd_edulevel.description AS educationLevel,
                    NULL AS averageMonthlyExpenditure,
                    NULL AS monthlyExpenses,
                    c.blacklisted_ind AS negativeClientStatus,
                    idt.description AS spouseIdentificationType,
                    NULL AS spouseIdentificationNumber,
                    c.mother_surname AS maidenName,
                    c.date_of_birth AS birthDate,
                    id_country.description AS birthCountry,
                    NULL AS birthPostalCode,
                    NULL AS birthHouseNumber,
                    NULL AS birthRegion,
                    NULL AS birthDistrict,
                    NULL AS birthWard,
                    NULL AS birthStreet,
                    idt.description AS identificationType,
                    id.id_no AS identificationNumber,
                    id.issue_date AS issuanceDate,
                    id.expiry_date AS expirationDate,
                    NULL AS issuancePlace,
                    NULL AS issuingAuthority,
                    c.short_name AS businessName,
                    NULL AS establishmentDate,
                    NULL AS businessRegistrationNumber,
                    NULL AS businessRegistrationDate,
                    NULL AS businessLicenseNumber,
                    afm.afm_no AS taxIdentificationNumber,
                    c.employer AS employerName,
                    NULL AS employerRegion,
                    NULL AS employerDistrict,
                    NULL AS employerWard,
                    c.employer_address AS employerStreet,
                    NULL AS employerHouseNumber,
                    NULL AS employerPostalCode,
                    gd_ccode.description AS businessNature,
                    c.mobile_tel AS mobileNumber,
                    c.mobile_tel2 AS alternativeMobileNumber,
                    c.telephone_1 AS fixedLineNumber,
                    c_address.fax_no AS faxNumber,
                    c.e_mail AS emailAddress,
                    c.internet_address AS socialMedia,
                    c_address.address_1 || ' ' || c_address.address_2 AS mainAddress,
                    c_address.address_1 AS street,
                    NULL AS houseNumber,
                    c_address.zip_code AS postalCode,
                    c_address.region AS region,
                    NULL AS district,
                    NULL AS ward,
                    c_country.description AS country,
                    w_address.address_1 AS workStreet,
                    NULL AS workHouseNumber,
                    w_address.zip_code AS workPostalCode,
                    w_address.region AS workRegion,
                    NULL AS workDistrict,
                    NULL AS workWard,
                    w_country.description AS workCountry
                FROM customer c
                LEFT JOIN cust_address c_address
                    ON c_address.fk_customercust_id = c.cust_id
                   AND c_address.communication_addr = '1'
                   AND c_address.entry_status = '1'
                LEFT JOIN w_address
                    ON w_address.fk_customercust_id = c.cust_id
                LEFT JOIN generic_detail c_country
                    ON c_address.fkgd_has_country = c_country.serial_num
                   AND c_address.fkgh_has_country = c_country.fk_generic_headpar
                LEFT JOIN generic_detail w_country
                    ON w_address.fkgd_has_country = w_country.serial_num
                   AND w_address.fkgh_has_country = w_country.fk_generic_headpar
                LEFT JOIN other_id id
                    ON id.fk_customercust_id = c.cust_id
                   AND (CASE WHEN id.serial_no IS NULL THEN '1' ELSE id.main_flag END = '1')
                LEFT JOIN generic_detail id_country
                    ON id.fkgh_has_been_issu = id_country.fk_generic_headpar
                   AND id.fkgd_has_been_issu = id_country.serial_num
                LEFT JOIN generic_detail idt
                    ON idt.fk_generic_headpar = id.fkgh_has_type
                   AND idt.serial_num = id.fkgd_has_type
                LEFT JOIN other_afm afm
                    ON afm.fk_customercust_id = c.cust_id
                   AND (CASE WHEN afm.serial_no IS NULL THEN '1'
                             ELSE CASE WHEN c.no_afm = '1' THEN CAST(afm.serial_no AS VARCHAR(2))
                                       ELSE afm.main_flag END
                        END = '1')
                LEFT JOIN customer_category cc_natio
                    ON cc_natio.fk_customercust_id = c.cust_id
                   AND cc_natio.fk_categorycategor = 'NATIONAL'
                   AND cc_natio.fk_generic_detafk = 'NATIO'
                LEFT JOIN generic_detail gd_natio
                    ON cc_natio.fk_generic_detafk = gd_natio.fk_generic_headpar
                   AND cc_natio.fk_generic_detaser = gd_natio.serial_num
                LEFT JOIN customer_category cc_citiz
                    ON cc_citiz.fk_customercust_id = c.cust_id
                   AND cc_citiz.fk_categorycategor = 'CITIZEN'
                   AND cc_citiz.fk_generic_detafk = 'CITIZ'
                LEFT JOIN generic_detail gd_citiz
                    ON cc_citiz.fk_generic_detafk = gd_citiz.fk_generic_headpar
                   AND cc_citiz.fk_generic_detaser = gd_citiz.serial_num
                LEFT JOIN customer_category cc_proff
                    ON cc_proff.fk_customercust_id = c.cust_id
                   AND cc_proff.fk_categorycategor = 'PROFES'
                   AND cc_proff.fk_generic_detafk = 'PROFF'
                LEFT JOIN generic_detail gd_proff
                    ON cc_proff.fk_generic_detafk = gd_proff.fk_generic_headpar
                   AND cc_proff.fk_generic_detaser = gd_proff.serial_num
                LEFT JOIN customer_category cc_ccode
                    ON cc_ccode.fk_customercust_id = c.cust_id
                   AND cc_ccode.fk_categorycategor = 'ACTIVITY'
                   AND cc_ccode.fk_generic_detafk = 'CCODE'
                LEFT JOIN generic_detail gd_ccode
                    ON cc_ccode.fk_generic_detafk = gd_ccode.fk_generic_headpar
                   AND cc_ccode.fk_generic_detaser = gd_ccode.serial_num
                LEFT JOIN customer_category cc_profcat
                    ON cc_profcat.fk_customercust_id = c.cust_id
                   AND cc_profcat.fk_categorycategor = 'PROFCAT'
                   AND cc_profcat.fk_generic_detafk = 'EMPTP'
                LEFT JOIN generic_detail gd_profcat
                    ON cc_profcat.fk_generic_detafk = gd_profcat.fk_generic_headpar
                   AND cc_profcat.fk_generic_detaser = gd_profcat.serial_num
                LEFT JOIN customer_category cc_edulevel
                    ON cc_edulevel.fk_customercust_id = c.cust_id
                   AND cc_edulevel.fk_categorycategor = 'EDULEVEL'
                   AND cc_edulevel.fk_generic_detafk = 'EDULV'
                LEFT JOIN generic_detail gd_edulevel
                    ON cc_edulevel.fk_generic_detafk = gd_edulevel.fk_generic_headpar
                   AND cc_edulevel.fk_generic_detaser = gd_edulevel.serial_num
                ORDER BY c.cust_id ASC
                FETCH FIRST 1000 ROWS ONLY
                """,
                timestamp_column='reportingDate',
                target_table='personalDataInformation',
                queue_name='personal_data_information_queue',
                processor_class='PersonalDataProcessor',
                batch_size=1000,
                poll_interval=10
            ),
            'microfinance_segment_loans': TableConfig(
                name='microfinance_segment_loans',
                query="""
                SELECT
                    VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHH24MI')     AS reportingDate,
                    COALESCE(
                            oid.ID_NO,
                            CAST(c.CUST_ID AS VARCHAR(20))
                    )                                                        AS customerIdentificationNumber,
                    CAST(la.FK_UNITCODE AS VARCHAR(10)) ||
                    CAST(la.ACC_TYPE AS VARCHAR(5)) ||
                    CAST(la.ACC_SN AS VARCHAR(15))                          AS accountNumber,

                    -- Client name
                    TRIM(
                            COALESCE(c.FIRST_NAME, '') || ' ' ||
                            COALESCE(c.MIDDLE_NAME, '') || ' ' ||
                            COALESCE(c.SURNAME, '')
                    )                                                        AS clientName,

                    ctl.CUSTOMER_TYPE                                        as clientType,
                    CASE
                        WHEN ctl.CUSTOMER_TYPE = 'Corporations' THEN 'Not Applicable'
                        ELSE
                            CASE
                                WHEN c.SEX = 'M' THEN 'Male'
                                WHEN c.SEX = 'F' THEN 'Female'
                                ELSE 'Not Applicable'
                                END
                        END                                                      AS gender,
                    CASE
                        WHEN ctl.CUSTOMER_TYPE = 'Corporations' THEN 0
                        ELSE
                            CASE WHEN c.DATE_OF_BIRTH IS NOT NULL
                                     THEN YEAR(CURRENT_DATE) - YEAR(c.DATE_OF_BIRTH)
                                 ELSE NULL
                                END
                        END                                                      AS age,
                    NULL                                                     AS disabilityStatus,
                    COALESCE(
                            agr.OLD_AGREEMEMT_NUM,
                            CAST(agr.FK_UNITCODE AS VARCHAR(10)) || '-' ||
                            CAST(agr.AGR_YEAR AS VARCHAR(4)) || '-' ||
                            CAST(agr.AGR_SN AS VARCHAR(10))
                    )                                                        AS loanNumber,
                    CASE
                        WHEN ctl.CUSTOMER_TYPE = 'Corporations' THEN 'Services'
                        ELSE 'Personal Loans'
                        END  AS loanIndustryClassification,

                    'Others'  AS loanSubIndustry,
                    CASE
                        WHEN ctl.CUSTOMER_TYPE = 'Corporations' THEN 'Business Group Loans'
                        ELSE 'Business Individual Loans'
                        END  AS microfinanceLoansType,
                    'Reducing Method'                                         as amortizationType,
                    CAST(la.FK_UNITCODE AS VARCHAR(10))                     AS branchCode,
                    COALESCE(
                            emp.FIRST_NAME || ' ' || emp.LAST_NAME,
                            agr.FK_BANKEMPLOYEEID,
                            'Not Assigned'
                    )                                                        AS loanOfficer,
                    COALESCE(
                            sup.FIRST_NAME || ' ' || sup.LAST_NAME,
                            agr.PRV_OFFICER,
                            'Not Assigned'
                    )                                                        AS loanSupervisor,
                    COALESCE(
                            la.VILLAGE_SN,
                            la.FKCUS_COOPERATIVE
                    )                                                        AS groupVillageNumber,
                    COALESCE(la.RECYCLING_FRQ, 1)                          AS cycleNumber,
                    COALESCE(curr.SHORT_DESCR, 'TZS')                      AS currency,
                    la.ACC_LIMIT_AMN                                                       as orgSanctionedAmount,
                    CASE
                        WHEN curr.SHORT_DESCR = 'USD'
                            THEN la.ACC_LIMIT_AMN
                        ELSE NULL
                        END                                                                  AS usdSanctionedAmount,
                    CASE
                        WHEN curr.SHORT_DESCR = 'USD'
                            THEN la.ACC_LIMIT_AMN * 2500
                        ELSE
                            la.ACC_LIMIT_AMN
                        END                                                                  AS tzsSanctionedAmount,
                    la.TOT_DRAWDOWN_AMN                                                    as orgDisbursedAmount,
                    CASE
                        WHEN curr.SHORT_DESCR = 'USD'
                            THEN la.TOT_DRAWDOWN_AMN
                        ELSE NULL
                        END                                                                  AS usdDisbursedAmount,
                    CASE
                        WHEN curr.SHORT_DESCR = 'USD'
                            THEN la.TOT_DRAWDOWN_AMN * 2500
                        ELSE
                            la.TOT_DRAWDOWN_AMN
                        END                                                                  AS tzsDisbursedAmount,
                    VARCHAR_FORMAT(la.DRAWDOWN_FST_DT, 'DDMMYYYYHH24MI')   AS disbursementDate,
                    VARCHAR_FORMAT(la.ACC_EXP_DT, 'DDMMYYYYHH24MI')        AS maturityDate,
                    CASE
                        WHEN la.LOAN_STATUS IN ('R', 'S')
                            THEN VARCHAR_FORMAT(la.LST_TRX_DT, 'DDMMYYYYHH24MI')
                        ELSE NULL
                        END                                                      AS restructuringDate,
                    CASE
                        WHEN la.LOAN_STATUS = '4'
                            THEN la.NRM_CAP_BAL + la.OV_CAP_BAL
                        ELSE 0
                        END                                                      AS writtenOffAmount,
                    la.INSTALL_COUNT                                        AS agreedLoanInstallments,
                    CASE
                        WHEN la.INSTALL_FREQ = 1 THEN 'Daily'
                        WHEN la.INSTALL_FREQ = 7 THEN 'Weekly'
                        WHEN la.INSTALL_FREQ = 14 THEN 'Bi-weekly'
                        WHEN la.INSTALL_FREQ = 30 THEN 'Monthly'
                        WHEN la.INSTALL_FREQ = 90 THEN 'Quarterly'
                        WHEN la.INSTALL_FREQ = 180 THEN 'Semi-annually'
                        WHEN la.INSTALL_FREQ = 365 THEN 'Annually'
                        ELSE 'Monthly'
                        END                                                      AS repaymentFrequency,
                    (la.NRM_CAP_BAL + la.OV_CAP_BAL)                                     AS orgOutstandingPrincipalAmount,
                    CASE
                        WHEN curr.SHORT_DESCR = 'USD' THEN (la.NRM_CAP_BAL + la.OV_CAP_BAL)
                        ELSE NULL
                        END                                                                  AS usdOutstandingPrincipalAmount,
                    CASE
                        WHEN curr.SHORT_DESCR = 'USD' THEN (la.NRM_CAP_BAL + la.OV_CAP_BAL) * 2500
                        ELSE (la.NRM_CAP_BAL + la.OV_CAP_BAL)
                        END                                                                  AS tzsOutstandingPrincipalAmount,
                    CASE
                        WHEN la.INSTALL_COUNT > 0
                            THEN la.INSTALL_COUNT - (la.NRM_INST_CNT + la.OV_INST_CNT)
                        ELSE 0
                        END                                                      AS loanInstallmentPaid,
                    0                                                        AS gracePeriodPaymentPrincipal,
                    COALESCE(br.BASE_RATE_PERC, 15.00)                     AS primeLendingRate,
                    COALESCE(
                            la.INTER_RATE_SPRD + COALESCE(br.BASE_RATE_PERC, 15.00),
                            20.00
                    )                                                        AS annualInterestRate,
                    COALESCE(
                            la.INTER_RATE_SPRD + COALESCE(br.BASE_RATE_PERC, 15.00) + 2.00,
                            22.00
                    )                                                        AS effectiveAnnualInterestRate,
                    la.INSTALL_FIRST_DT                                     AS firstInstallmentPaymentDate,
                    CASE
                        WHEN la.LOAN_STATUS IN ('R', 'S') THEN 'Restructured'
                        ELSE 'Non-restructured'
                        END                                                      AS loanFlagType,

                    CASE
                        WHEN la.OV_EXP_DT IS NOT NULL AND la.OV_EXP_DT < CURRENT_DATE
                            THEN DAYS(CURRENT_DATE) - DAYS(la.OV_EXP_DT)
                        ELSE 0
                        END                                                      AS pastDueDays,
                    (la.OV_CAP_BAL + la.OV_RL_NRM_INT_BAL + la.OV_RL_PNL_INT_BAL +
                     la.OV_COM_BAL + la.OV_EXP_BAL)                        AS pastDueAmount,
                    (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL +
                     la.OV_ACR_PNL_INT_BAL)                                AS orgAccruedInterestAmount,
                    CASE
                        WHEN curr.SHORT_DESCR = 'USD'
                            THEN (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL)
                        WHEN curr.SHORT_DESCR = 'TZS'
                            THEN (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL) / 2500.50
                        ELSE (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL) / 2500.50
                        END                                                      AS usdAccruedInterestAmount,
                    CASE
                        WHEN curr.SHORT_DESCR = 'USD'
                            THEN (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL) * 2730.50
                        ELSE (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL)
                        END                                                      AS tzsAccruedInterestAmount,
                    'Current'                                                                AS assetClassificationCategory,
                    CASE
                        WHEN la.LOAN_STATUS = 'W' THEN la.NRM_CAP_BAL + la.OV_CAP_BAL
                        WHEN la.OV_EXP_DT IS NULL OR DAYS(CURRENT_DATE) - DAYS(la.OV_EXP_DT) <= 30
                            THEN (la.NRM_CAP_BAL + la.OV_CAP_BAL) * 0.01
                        WHEN DAYS(CURRENT_DATE) - DAYS(la.OV_EXP_DT) <= 90
                            THEN (la.NRM_CAP_BAL + la.OV_CAP_BAL) * 0.05
                        WHEN DAYS(CURRENT_DATE) - DAYS(la.OV_EXP_DT) <= 180
                            THEN (la.NRM_CAP_BAL + la.OV_CAP_BAL) * 0.20
                        WHEN DAYS(CURRENT_DATE) - DAYS(la.OV_EXP_DT) <= 365
                            THEN (la.NRM_CAP_BAL + la.OV_CAP_BAL) * 0.50
                        ELSE (la.NRM_CAP_BAL + la.OV_CAP_BAL) * 1.00
                        END                                                      AS allowanceProbableLoss,
                    0                                                     AS botProvision
                FROM LOAN_ACCOUNT la
                LEFT JOIN CUSTOMER c ON c.CUST_ID = la.CUST_ID
                LEFT JOIN CUSTOMER_TYPES_LOOKUP ctl ON ctl.CUSTOMER_TYPE_CODE = c.CUST_TYPE
                LEFT JOIN OTHER_ID oid ON oid.FK_CUSTOMERCUST_ID = c.CUST_ID
                LEFT JOIN AGREEMENT agr ON agr.FK_UNITCODE = la.FK_AGREEMENTFK_UNI
                    AND agr.AGR_YEAR = la.FK_AGREEMENTAGR_YE
                    AND agr.AGR_SN = la.FK_AGREEMENTAGR_SN
                    AND agr.AGR_MEMBERSHIP_SN = la.FK_AGREEMENTAGR_ME
                LEFT JOIN PRODUCT p ON p.ID_PRODUCT = la.FK_LOANFK_PRODUCTI
                LEFT JOIN CURRENCY curr ON curr.ID_CURRENCY = la.FKCUR_IS_MOVED_IN
                LEFT JOIN BASE_RATE br ON br.FK_GH_PAR_TYPE = 'PRIME'
                    AND br.VALIDITY_DATE <= CURRENT_DATE
                    AND br.ENTRY_STATUS = '1'
                LEFT JOIN BANKEMPLOYEE emp ON emp.ID = agr.FK_BANKEMPLOYEEID
                LEFT JOIN BANKEMPLOYEE sup ON sup.ID = agr.FK0BANKEMPLOYEEID
                LEFT JOIN GENERIC_DETAIL gd_industry ON gd_industry.FK_GENERIC_HEADPAR = la.FKGH_CATEGORY
                    AND gd_industry.SERIAL_NUM = la.FKGD_CATEGORY
                LEFT JOIN GENERIC_DETAIL gd_subind ON gd_subind.FK_GENERIC_HEADPAR = la.FKGH_HAS_AS_FINANC
                    AND gd_subind.SERIAL_NUM = la.FKGD_HAS_AS_FINANC
                LEFT JOIN COLLATERAL_TABLE ct ON (ct.PROFITS_ACCOUNT_1 = CAST(la.FK_UNITCODE AS VARCHAR(10)) ||
                                                  CAST(la.ACC_TYPE AS VARCHAR(5)) ||
                                                  CAST(la.ACC_SN AS VARCHAR(15))
                    OR ct.CUST_ID_1 = la.CUST_ID)
                LEFT JOIN CURRENCY curr_coll ON curr_coll.ID_CURRENCY = ct.CURRENCY_ID
                LEFT JOIN GENERIC_DETAIL gd_coll ON gd_coll.FK_GENERIC_HEADPAR = ct.GD_PAR_TYPE_1
                    AND gd_coll.SERIAL_NUM = ct.GD_SERIAL_NUM_1
                WHERE
                    la.ACC_STATUS IN ('1', '6')
                  AND (
                    p.PRODUCT_TYPE IN ('MICRO', 'SME', 'CONSUMER', 'AGRI')
                        OR p.DESCRIPTION LIKE '%MICRO%'
                        OR p.DESCRIPTION LIKE '%DIGITAL%'
                        OR p.DESCRIPTION LIKE '%MOBILE%'
                        OR la.ACC_LIMIT_AMN <= 50000000
                    )
                  AND NOT (
                    la.LOAN_STATUS = 'P'
                        AND (la.NRM_CAP_BAL + la.OV_CAP_BAL) = 0
                        AND la.LST_TRX_DT < CURRENT_DATE - 30 DAYS
                    )
                ORDER BY la.FK_UNITCODE, la.ACC_TYPE, la.ACC_SN
                FETCH FIRST 1000 ROWS ONLY
                """,
                timestamp_column='disbursementDate',
                target_table='microfinanceSegmentLoans',
                queue_name='microfinance_segment_loans_queue',
                processor_class='MicrofinanceSegmentLoansProcessor',
                batch_size=1000,
                poll_interval=10
            ),
            'digitalCredit': TableConfig(
                name='digitalCredit',
                query="""
                SELECT 
                    CURRENT_TIMESTAMP AS reportingDate,
                    c.CUST_ID AS customerName,
                    CASE 
                        WHEN c.SEX = 'M' THEN 'Male'
                        WHEN c.SEX = 'F' THEN 'Female'
                        ELSE NULL
                    END AS gender,
                    NULL AS disabilityStatus,
                    COALESCE(
                        CASE WHEN TRIM(c.EMPLOYEE_ID) = '' THEN NULL ELSE TRIM(c.EMPLOYEE_ID) END,
                        CAST(c.CUST_ID AS VARCHAR(20))
                    ) AS customerIdentificationNumber,
                    COALESCE(CAST(bp.BANK_CODE AS VARCHAR(10)), 'MCB') AS institutionCode,
                    COALESCE(la.FK_UNITCODE, c.FKUNIT_IS_SERVICED) AS branchCode,
                    CASE 
                        WHEN UPPER(p.DESCRIPTION) LIKE '%MPESA%' OR UPPER(p.DESCRIPTION) LIKE '%M-PESA%' THEN 'M-Pesa'
                        WHEN UPPER(p.DESCRIPTION) LIKE '%AIRTEL%' THEN 'Airtel Money'
                        WHEN UPPER(p.DESCRIPTION) LIKE '%TIGO%' THEN 'Tigo Pesa'
                        WHEN UPPER(p.DESCRIPTION) LIKE '%HALO%' THEN 'Halopesa'
                        WHEN UPPER(p.DESCRIPTION) LIKE '%T-PESA%' OR UPPER(p.DESCRIPTION) LIKE '%TPESA%' THEN 'T-Pesa'
                        WHEN UPPER(p.DESCRIPTION) LIKE '%EZY%' THEN 'EzyPesa'
                        WHEN UPPER(p.DESCRIPTION) LIKE '%AZAM%' THEN 'AzamPesa'
                        WHEN UPPER(p.DESCRIPTION) LIKE '%JUMO%' THEN 'Jumo'
                        WHEN UPPER(p.DESCRIPTION) LIKE '%LAINA%' THEN 'Laina Finance Limited'
                        ELSE 'Internal Facilitated'
                    END AS servicesFacilitator,
                    COALESCE(p.DESCRIPTION, 'DIGITAL_CREDIT') AS productName,
                    COALESCE(la.TOT_DRAWDOWN_AMN, la.ACC_LIMIT_AMN, 0) AS tzsLoanDisbursedAmount,
                    COALESCE(la.ACC_OPEN_DT, la.DRAWDOWN_FST_DT) AS loanDisbursementDate,
                    COALESCE(la.NRM_CAP_BAL + la.OV_CAP_BAL, 0) AS tzsLoanBalance,
                    COALESCE(la.ACC_EXP_DT, la.DRAWDOWN_EXP_DT) AS maturityDate,
                    CAST(la.FK_UNITCODE AS VARCHAR(10)) || '-' || CAST(la.ACC_TYPE AS VARCHAR(5)) || '-' || CAST(la.ACC_SN AS VARCHAR(10)) AS loanId,
                    la.LST_TRX_DT AS lastDepositDate,
                    NULL AS lastDepositAmount,
                    COALESCE(la.INSTALL_COUNT, 12) AS paymentsInstallment,
                    CASE 
                        WHEN la.INSTALL_FREQ = 0 THEN 'ON_DEMAND'
                        WHEN la.INSTALL_FREQ = 1 THEN 'DAILY'
                        WHEN la.INSTALL_FREQ BETWEEN 2 AND 6 THEN 'WEEKLY'
                        WHEN la.INSTALL_FREQ = 6 THEN 'BI_WEEKLY'
                        WHEN la.INSTALL_FREQ = 7 THEN 'WEEKLY'
                        WHEN la.INSTALL_FREQ BETWEEN 8 AND 11 THEN 'WEEKLY'
                        WHEN la.INSTALL_FREQ = 12 THEN 'MONTHLY'
                        WHEN la.INSTALL_FREQ BETWEEN 13 AND 23 THEN 'MONTHLY'
                        WHEN la.INSTALL_FREQ = 24 THEN 'BI_WEEKLY'
                        WHEN la.INSTALL_FREQ BETWEEN 25 AND 35 THEN 'MONTHLY'
                        WHEN la.INSTALL_FREQ = 36 THEN 'MONTHLY'
                        WHEN la.INSTALL_FREQ BETWEEN 37 AND 47 THEN 'MONTHLY'
                        WHEN la.INSTALL_FREQ = 48 THEN 'BI_WEEKLY'
                        WHEN la.INSTALL_FREQ = 49 THEN 'MONTHLY'
                        ELSE 'MONTHLY'
                    END AS repaymentsFrequency,
                    CASE 
                        WHEN la.INSTALL_COUNT = 1 THEN 'Bullet Loan'
                        WHEN la.INSTALL_COUNT > 1 AND COALESCE(la.INTER_RATE_SPRD, 0) > 0 THEN 'Reducing Method'
                        WHEN la.INSTALL_COUNT > 1 THEN 'Fixed Installment'
                        ELSE 'Reducing Method'
                    END AS loanAmotizationType,
                    NULL AS cycleNumber,
                    COALESCE(la.TOT_CAP_AMN + la.TOT_NRM_INT_AMN + la.TOT_PNL_INT_AMN, 0) AS loanAmountPaid,
                    CASE 
                        WHEN la.OV_EXP_DT IS NOT NULL AND la.OV_CAP_BAL > 0 THEN la.OV_EXP_DT
                        ELSE NULL
                    END AS deliquenceDate,
                    NULL AS restructuringDate,
                    COALESCE(la.INTER_RATE_SPRD, 15.0) AS interestRate,
                    CASE 
                        WHEN la.OV_EXP_DT IS NOT NULL AND la.OV_CAP_BAL > 0 
                        THEN DAYS(CURRENT_DATE) - DAYS(la.OV_EXP_DT)
                        ELSE 0
                    END AS pastDueDays,
                    COALESCE(la.OV_CAP_BAL, 0) AS pastDueAmount,
                    'TZS' AS currency,
                    COALESCE(la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL, 0) AS orgAccruedInterest,
                    COALESCE(la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL, 0) AS tzsAccruedInterest,
                    NULL AS usdAccruedInterest,
                    CASE 
                        WHEN COALESCE(la.OV_CAP_BAL, 0) <= 0 THEN 'Current'
                        WHEN la.OV_EXP_DT IS NULL THEN 'Current'
                        WHEN DAYS(CURRENT_DATE) - DAYS(la.OV_EXP_DT) BETWEEN 1 AND 30 THEN 'EspeciallyMentioned'
                        WHEN DAYS(CURRENT_DATE) - DAYS(la.OV_EXP_DT) BETWEEN 31 AND 90 THEN 'Substandard'
                        WHEN DAYS(CURRENT_DATE) - DAYS(la.OV_EXP_DT) BETWEEN 91 AND 180 THEN 'Doubtful'
                        WHEN DAYS(CURRENT_DATE) - DAYS(la.OV_EXP_DT) > 180 THEN 'Loss'
                        ELSE 'Current'
                    END AS assetClassification,
                    0 AS allowanceProbableLoss,
                    0 AS botProvision,
                    COALESCE(la.OV_URL_NRM_INT_BAL + la.OV_URL_PNL_INT_BAL, 0) AS interestSuspended
                FROM CUSTOMER c
                INNER JOIN LOAN_ACCOUNT la ON c.CUST_ID = la.CUST_ID
                LEFT JOIN PRODUCT p ON la.FK_LOANFK_PRODUCTI = p.ID_PRODUCT
                LEFT JOIN BANK_PARAMETERS bp ON 1=1
                WHERE 
                    la.LOAN_STATUS IS NOT NULL
                    AND la.ACC_OPEN_DT IS NOT NULL
                    AND la.ACC_OPEN_DT >= '2018-01-01'
                    AND (
                        c.MOBILE_TEL IS NOT NULL
                        OR la.TOT_DRAWDOWN_AMN <= 10000000
                        OR la.INSTALL_COUNT <= 60
                        OR la.ACC_OPEN_DT >= '2020-01-01'
                    )
                ORDER BY la.ACC_OPEN_DT DESC, c.CUST_ID
                FETCH FIRST 1000 ROWS ONLY
                """,
                timestamp_column='loanDisbursementDate',
                target_table='digitalCredit',
                queue_name='digital_credit_queue',
                processor_class='DigitalCreditProcessor',
                batch_size=1000,
                poll_interval=10
            ),
            'atmTransaction': TableConfig(
                name='atmTransaction',
                query="""
                select
                    CURRENT_TIMESTAMP as reportingDate,
                    atx.TERMINAL as atmCode,
                    atx.TUN_DATE as transactionDate,
                    atx.REFERENCE_NUMBER as transactionId,
                    CASE
                        WHEN atx.PROCESSING_CODE IN ('001000','002000','011000','011096','012000') THEN 'Cash Withdrawal'
                        WHEN atx.PROCESSING_CODE = '219610' THEN 'Purchase / Payment'
                        WHEN atx.PROCESSING_CODE IN ('311000','312000','381000','382000') THEN 'Non-Financial'
                        ELSE 'Others'
                    END as transactionNature,
                    'TZS' as currency,
                    atx.TRANSACTION_AMOUNT as orgTransactionAmount,
                    atx.TRANSACTION_AMOUNT as tzsTransactionAmount,
                    'Card and Mobile Based' as atmChannel,
                   DECIMAL(atx.TRANSACTION_AMOUNT * 0.18, 15, 2) AS valueAddedTaxAmount,
                    0 as exciseDutyAmount,
                    0 as electronicLevyAmount
                FROM ATM_TRX_RECORDING atx
                LEFT JOIN ATM_PROCESS_CODE pc ON pc.ISO_CODE = atx.PROCESSING_CODE 
                WHERE TERMINAL in('MWL01001','MWL01002')
                ORDER BY atx.TUN_DATE DESC
                FETCH FIRST 1000 ROWS ONLY
                """,
                timestamp_column='TUN_DATE',
                target_table='atmTransaction',
                queue_name='atm_transaction_queue',
                processor_class='AtmTransactionProcessor',
                batch_size=1000,
                poll_interval=10
            ),
            'chequeClearing': TableConfig(
                name='chequeClearing',
                query="""
                SELECT
                    CURRENT_TIMESTAMP as reportingDate,
                    cfc.CHEQUE_NUMBER AS chequeNumber,
                    RTRIM( LTRIM(
                        COALESCE(ic.FIRST_NAME, '') || ' ' ||
                        COALESCE(ic.MIDDLE_NAME, '') || ' ' ||
                        COALESCE(ic.SURNAME, '')
                    )
                ) AS issuerName,
                    COALESCE(bic.BIC, 'UNKNOWN') AS issuerBankerCode,
                        RTRIM( LTRIM(
                        COALESCE(pc.FIRST_NAME, '') || ' ' ||
                        COALESCE(pc.MIDDLE_NAME, '') || ' ' ||
                        COALESCE(pc.SURNAME, '')
                    )
                ) AS payeeName,
                    ppa.ACCOUNT_NUMBER as payeeAccountNumber,
                    cfc.ISSUE_DATE as chequeDate,
                    cfc.TRX_DATE as transactionDate,
                    cfc.BEAR_PAYMENT_DATE as settlementDate,
                    0 as allowanceProbableLoss,
                    0 as botProvision,
                    cu.SHORT_DESCR as currency,
                    CAST(da.OPENING_BALANCE AS DECFLOAT) AS orgAmountOpening,
                    0 as usdAmountOpening,
                    CAST(da.OPENING_BALANCE AS DECFLOAT) AS tzsAmountOpening,
                    CAST(cfc.CHEQUE_AMOUNT AS DECFLOAT) AS orgAmountPayment,
                    0 as usdAmountPayment,
                    CAST(cfc.CHEQUE_AMOUNT AS DECFLOAT) AS tzsAmountPayment,
                    CAST(da.BOOK_BALANCE AS DECFLOAT) AS orgAmountBalance,
                    0 as usdAmountBalance,
                    CAST(da.BOOK_BALANCE AS DECFLOAT) AS tzsAmountBalance
                    FROM
                        CHEQUES_FOR_COLLEC AS cfc
                    JOIN DEPOSIT_ACCOUNT da
                        ON VARCHAR(da.ACCOUNT_NUMBER) = LTRIM(CHAR(cfc.CHEQUE_NUMBER, 20), '0')
                    JOIN
                        CUSTOMER ic ON da.FK_CUSTOMERCUST_ID = ic.CUST_ID
                    JOIN
                        CURRENCY cu ON cfc.FK_CURRENCYID_CURR = cu.ID_CURRENCY
                    JOIN
                        CUSTOMER pc ON cfc.FK_CUSTOMERCUST_ID = pc.CUST_ID
                    LEFT JOIN BANK_BIC_LOOKUP bic
                       ON UPPER(TRIM(cfc.DRAWN_BANK)) = UPPER(TRIM(bic.BANK_NAME))
                    JOIN
                        PROFITS_ACCOUNT ppa ON ppa.CUST_ID = pc.CUST_ID
                ORDER BY cfc.TRX_DATE DESC
                FETCH FIRST 1000 ROWS ONLY
                """,
                timestamp_column='TRX_DATE',
                target_table='chequeClearing',
                queue_name='cheque_clearing_queue',
                processor_class='ChequeClearingProcessor',
                batch_size=1000,
                poll_interval=10
            ),
            'bankerChequesDrafts': TableConfig(
                name='bankerChequesDrafts',
                query="""
                WITH pa_unique AS (
                    SELECT DEP_ACC_NUMBER,
                           CUST_ID,
                           LIMIT_CURRENCY,
                           ACCOUNT_NUMBER,
                           ROW_NUMBER() OVER (PARTITION BY DEP_ACC_NUMBER ORDER BY ACCOUNT_NUMBER) AS rn
                    FROM PROFITS_ACCOUNT
                )
                SELECT CURRENT_TIMESTAMP AS reportingDate,
                       nr.CUST_ID        AS customerIdentificationNumber,
                       cu.NAME_STANDARD  AS customerName,
                       NULL              AS beneficiaryName,
                       NULL              AS checkNumber,
                       nr.TRX_DATE       AS transactionDate,
                       nr.ISSUE_DATE     AS valueDate,
                       NULL              AS maturityDate,
                       c.SHORT_DESCR     AS currency,
                       nr.CHEQUE_AMOUNT  AS orgAmount,
                       DECIMAL(
                           CASE
                               WHEN c.SHORT_DESCR = 'USD'
                                   THEN nr.CHEQUE_AMOUNT
                               WHEN c.SHORT_DESCR = 'TZS'
                                   THEN nr.CHEQUE_AMOUNT / 2500.9
                               ELSE 0
                           END,
                           18, 2
                       ) AS usdAmount,
                       DECIMAL(
                           CASE
                               WHEN c.SHORT_DESCR = 'TZS'
                                   THEN nr.CHEQUE_AMOUNT
                               WHEN c.SHORT_DESCR = 'USD'
                                   THEN nr.CHEQUE_AMOUNT * 2500.9
                               ELSE 0
                           END,
                           18, 2
                       ) AS tzsAmount
                FROM (
                    SELECT cbi.*,
                           pa.CUST_ID,
                           pa.LIMIT_CURRENCY
                    FROM CHEQUE_BOOK_ITEM cbi
                    LEFT JOIN pa_unique pa
                           ON pa.DEP_ACC_NUMBER = cbi.ACCOUNT_NUMBER
                          AND pa.rn = 1
                ) nr
                LEFT JOIN PROFITS.W_DIM_CUSTOMER cu
                       ON cu.CUST_ID = nr.CUST_ID
                LEFT JOIN CURRENCY c
                       ON c.ID_CURRENCY = nr.LIMIT_CURRENCY
                WHERE nr.CHEQUE_AMOUNT > 0
                ORDER BY nr.TRX_DATE DESC
                FETCH FIRST 1000 ROWS ONLY
                """,
                timestamp_column='TRX_DATE',
                target_table='bankerChequesDrafts',
                queue_name='banker_cheques_drafts_queue',
                processor_class='BankerChequesDraftsProcessor',
                batch_size=1000,
                poll_interval=10
            ),
            'incomeStatement': TableConfig(
                name='incomeStatement',
                query="""
                SELECT
                    CURRENT TIMESTAMP AS reportingDate,
                    SUM(CASE WHEN gl.EXTERNAL_GLACCOUNT LIKE '4%'
                             AND gl.EXTERNAL_GLACCOUNT NOT LIKE '40%'
                             AND gl.EXTERNAL_GLACCOUNT NOT LIKE '41%'
                             THEN COALESCE(gte.DC_AMOUNT, 0)
                             ELSE 0 END) AS interestIncome,
                    SUM(CASE WHEN gl.EXTERNAL_GLACCOUNT LIKE '6%'
                             THEN COALESCE(gte.DC_AMOUNT, 0)
                             ELSE 0 END) AS interestExpense,
                    SUM(CASE WHEN gl.EXTERNAL_GLACCOUNT IN ('705190002','705190003')
                             THEN COALESCE(gte.DC_AMOUNT, 0)
                             ELSE 0 END) AS badDebtsWrittenOffNotProvided,
                    SUM(CASE WHEN gl.EXTERNAL_GLACCOUNT IN ('705190001','705190002','705190003')
                             THEN COALESCE(gte.DC_AMOUNT, 0)
                             ELSE 0 END) AS provisionBadDoubtfulDebts,
                    0 AS impairmentsInvestments,
                    SUM(CASE WHEN gl.EXTERNAL_GLACCOUNT LIKE '50%'
                             THEN COALESCE(gte.DC_AMOUNT, 0)
                             ELSE 0 END) AS nonInterestIncome,
                    SUM(CASE WHEN gl.EXTERNAL_GLACCOUNT LIKE '7%'
                             THEN COALESCE(gte.DC_AMOUNT, 0)
                             ELSE 0 END) AS nonInterestExpenses,
                    0 AS incomeTaxProvision,
                    0 AS extraordinaryCreditsCharge,
                    SUM(CASE WHEN gl.EXTERNAL_GLACCOUNT IN ('505010001','505070001')
                             THEN COALESCE(gte.DC_AMOUNT, 0)
                             ELSE 0 END) AS nonCoreCreditsCharges,
                    SUM(CASE WHEN gl.EXTERNAL_GLACCOUNT LIKE '4%'
                             AND gl.EXTERNAL_GLACCOUNT NOT LIKE '40%'
                             AND gl.EXTERNAL_GLACCOUNT NOT LIKE '41%'
                             THEN COALESCE(gte.DC_AMOUNT, 0)
                             ELSE 0 END) AS amountInterestIncome,
                    SUM(CASE WHEN gl.EXTERNAL_GLACCOUNT LIKE '6%'
                             THEN COALESCE(gte.DC_AMOUNT, 0)
                             ELSE 0 END) AS amountInterestExpenses,
                    SUM(CASE WHEN gl.EXTERNAL_GLACCOUNT LIKE '50%'
                             THEN COALESCE(gte.DC_AMOUNT, 0)
                             ELSE 0 END) AS amountNonInterestIncome,
                    SUM(CASE WHEN gl.EXTERNAL_GLACCOUNT LIKE '7%'
                             THEN COALESCE(gte.DC_AMOUNT, 0)
                             ELSE 0 END) AS amountNonInterestExpenses,
                    SUM(CASE WHEN gl.EXTERNAL_GLACCOUNT IN ('505010001','505070001')
                             THEN COALESCE(gte.DC_AMOUNT, 0)
                             ELSE 0 END) AS amountnonCoreCreditsCharges
                FROM GLI_TRX_EXTRACT gte
                LEFT JOIN GLG_ACCOUNT gl ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO
                WHERE gl.EXTERNAL_GLACCOUNT LIKE '4%' 
                   OR gl.EXTERNAL_GLACCOUNT LIKE '5%'
                   OR gl.EXTERNAL_GLACCOUNT LIKE '6%'
                   OR gl.EXTERNAL_GLACCOUNT LIKE '7%'
                """,
                timestamp_column='reportingDate',
                target_table='incomeStatement',
                queue_name='income_statement_queue',
                processor_class='IncomeStatementProcessor',
                batch_size=1,
                poll_interval=10
            ),
            'interBankLoanReceivable': TableConfig(
                name='interBankLoanReceivable',
                query="""
                select CURRENT_TIMESTAMP AS reportingDate,
                       LTRIM(RTRIM(id.ID_NO)) AS customerIdentificationNumber,
                       LTRIM(RTRIM(pa.ACCOUNT_NUMBER)) AS accountNumber,
                       LTRIM(RTRIM(wela.CUSTOMER_NAME)) AS clientName,
                       cl.COUNTRY_CODE as borrowerCountry,
                       null as ratingStatus,
                       null as crRatingBorrower,
                       null as gradesUnratedBanks,
                       lccd.GENDER as gender,
                       lccd.GENDER as disability,
                       ctl.CUSTOMER_TYPE as clientType,
                       null as clientSubType,
                       null as groupName,
                       null as groupCode,
                       'No relation' as relatedParty,
                       'Direct' as relationshipCategory,
                       wela.ACCOUNT_NUMBER as loanNumber,
                       CASE
                           WHEN PRODUCT_DESC LIKE '%PERSONAL%' AND PRODUCT_DESC LIKE '%LOAN%'
                               THEN 'Personal Loan'
                           WHEN PRODUCT_DESC LIKE '%BUSINESS%' AND PRODUCT_DESC LIKE '%LOAN%'
                               THEN 'Business Loan'
                           WHEN PRODUCT_DESC LIKE '%MORTAGE%' AND PRODUCT_DESC LIKE '%LOAN%'
                               THEN 'Mortage Loan'
                           ELSE 'Unknown'
                           END AS loanType,
                       'OtherServices' as loanEconomicActivity,
                       'Existing' as loanPhase,
                       'NotSpecified' as transferStatus,
                       CASE
                           WHEN wela.PRODUCT_DESC LIKE '%MORTGAGE%' AND wela.PRODUCT_DESC LIKE '%LOAN%'
                               THEN
                               CASE
                                   WHEN GG.DESCRIPTION LIKE '%Development%' THEN 'Improvement'
                                   WHEN GG.DESCRIPTION LIKE '%Purchase%' THEN 'Acquisition'
                                   WHEN GG.DESCRIPTION LIKE '%Construct%' THEN 'Construction'
                                   WHEN GG.DESCRIPTION LIKE '%Others%' THEN 'Others'
                                   ELSE 'Unknown'
                                   END
                           END AS purposeMortgage,
                       GG.DESCRIPTION as purposeOtherLoans,
                       'Others' as sourceFundMortgage,
                       'Reducing Method' as amortizationType,
                       wela.FK_UNITCODE as branchCode,
                       wela.LOAN_OFFICER_NAME as loanOfficer,
                       null as loanSupervisor,
                       null as groupVillageNumber,
                       null as cycleNumber,
                       wela.INSTALL_COUNT as loanInstallment,
                       CASE
                           WHEN wela.INSTALL_FREQ = 1 THEN 'Daily'
                           WHEN wela.INSTALL_FREQ = 7 THEN 'Weekly'
                           WHEN wela.INSTALL_FREQ = 14 THEN 'Bi-weekly'
                           WHEN wela.INSTALL_FREQ = 30 THEN 'Monthly'
                           WHEN wela.INSTALL_FREQ = 90 THEN 'Quarterly'
                           WHEN wela.INSTALL_FREQ = 180 THEN 'Semi-annually'
                           WHEN wela.INSTALL_FREQ = 365 THEN 'Annually'
                           ELSE 'Monthly'
                           END AS repaymentFrequency,
                       wela.CURRENCY as currency,
                       wela.ACC_OPEN_DT as contractDate,
                       wela.ACC_LIMIT_AMN as orgSanctionedAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD'
                               THEN wela.ACC_LIMIT_AMN
                           ELSE NULL
                           END AS usdSanctionedAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD'
                               THEN wela.ACC_LIMIT_AMN * 2500
                           ELSE
                               wela.ACC_LIMIT_AMN
                           END AS tzsSanctionedAmount,
                       wela.TOT_DRAWDOWN_AMN as orgDisbursedAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD'
                               THEN wela.TOT_DRAWDOWN_AMN
                           ELSE NULL
                           END AS usdDisbursedAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD'
                               THEN wela.TOT_DRAWDOWN_AMN * 2500
                           ELSE
                               wela.TOT_DRAWDOWN_AMN
                           END AS tzsDisbursedAmount,
                       wela.DRAWDOWN_FST_DT AS disbursementDate,
                       wela.ACC_EXP_DT AS maturityDate,
                       COALESCE(wela.WRITE_OFF_DATE, wela.OV_EXP_DT, wela.ACC_EXP_DT) AS realEndDate,
                       (wela.NRM_BALANCE + wela.OV_BALANCE) AS orgOutstandingPrincipalAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD' THEN (wela.NRM_BALANCE + wela.OV_BALANCE)
                           ELSE NULL
                           END AS usdOutstandingPrincipalAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD' THEN (wela.NRM_BALANCE + wela.OV_BALANCE) * 2500
                           ELSE (wela.NRM_BALANCE + wela.OV_BALANCE)
                           END AS tzsOutstandingPrincipalAmount,
                       wela.INSTALLMENT_AMOUNT AS orgInstallmentAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD' THEN wela.INSTALLMENT_AMOUNT
                           ELSE NULL
                           END AS usdInstallmentAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD' THEN wela.INSTALLMENT_AMOUNT * 2500
                           ELSE wela.INSTALLMENT_AMOUNT
                           END AS tzsInstallmentAmount,
                       wela.INSTA_PAID as loanInstallmentPaid,
                       null as gracePeriodPaymentPrincipal,
                       wela.SELECTED_BANK_RATE AS primeLendingRate,
                       null AS interestPricingMethod,
                       wela.FINAL_INTEREST AS annualInterestRate,
                       null AS effectiveAnnualInterestRate,
                       null AS loanFlagType,
                       null AS restructuringDate,
                       wela.OVERDUE_DAYS AS pastDueDays,
                       wela.OV_BALANCE AS pastDueAmount,
                       null AS internalRiskGroup,
                       wela.NRM_ACR_INT_BAL + wela.OV_ACR_NRM_INT_BAL + wela.OV_ACR_PNL_INT_BAL AS orgAccruedInterestAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD' THEN wela.NRM_ACR_INT_BAL + wela.OV_ACR_NRM_INT_BAL + wela.OV_ACR_PNL_INT_BAL
                           ELSE NULL
                           END AS usdAccruedInterestAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD' THEN (wela.NRM_ACR_INT_BAL + wela.OV_ACR_NRM_INT_BAL + wela.OV_ACR_PNL_INT_BAL) * 2500
                           ELSE wela.NRM_ACR_INT_BAL + wela.OV_ACR_NRM_INT_BAL + wela.OV_ACR_PNL_INT_BAL
                           END AS tzsAccruedInterestAmount,
                       wela.OV_RL_PNL_INT_BAL + wela.OV_URL_PNL_INT_BAL AS orgPenaltyChargedAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD' THEN wela.OV_RL_PNL_INT_BAL + wela.OV_URL_PNL_INT_BAL
                           ELSE NULL
                           END AS usdPenaltyChargedAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD' THEN (wela.OV_RL_PNL_INT_BAL + wela.OV_URL_PNL_INT_BAL) * 2500
                           ELSE wela.OV_RL_PNL_INT_BAL + wela.OV_URL_PNL_INT_BAL
                           END AS tzsPenaltyChargedAmount,
                       COALESCE(wela.TOT_PNL_INT_AMN, 0) AS orgPenaltyPaidAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD' THEN COALESCE(wela.TOT_PNL_INT_AMN, 0)
                           ELSE NULL
                           END AS usdPenaltyPaidAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD' THEN COALESCE(wela.TOT_PNL_INT_AMN, 0) * 2500
                           ELSE COALESCE(wela.TOT_PNL_INT_AMN, 0)
                           END AS tzsPenaltyPaidAmount,
                       wela.TOT_COMMISSION_AMN AS orgLoanFeesChargedAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD' THEN wela.TOT_COMMISSION_AMN
                           ELSE NULL
                           END AS usdLoanFeesChargedAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD' THEN wela.TOT_COMMISSION_AMN * 2500
                           ELSE wela.TOT_COMMISSION_AMN
                           END AS tzsLoanFeesChargedAmount,
                       wela.TOT_EXPENSE_AMN AS orgLoanFeesPaidAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD' THEN wela.TOT_EXPENSE_AMN
                           ELSE NULL
                           END AS usdLoanFeesPaidAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD' THEN wela.TOT_EXPENSE_AMN * 2500
                           ELSE wela.TOT_EXPENSE_AMN
                           END AS tzsLoanFeesPaidAmount,
                       wela.INSTALL_FIXED_AMN AS orgTotMonthlyPaymentAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD' THEN wela.INSTALL_FIXED_AMN
                           ELSE NULL
                           END AS usdTotMonthlyPaymentAmount,
                       CASE
                           WHEN wela.CURRENCY = 'USD' THEN wela.INSTALL_FIXED_AMN * 2500
                           ELSE wela.INSTALL_FIXED_AMN
                           END AS tzsTotMonthlyPaymentAmount,
                       wela.CLOAN_CATEGORY_DESCRIPTION AS sectorSnaClassification,
                       'Current' AS assetClassificationCategory,
                       wela.ACC_STATUS AS negStatusContract,
                       wela.CUST_TYPE AS customerRole,
                       wela.PROVISION_AMOUNT AS allowanceProbableLoss,
                       wela.PROVISION_AMN AS botProvision,
                       null AS tradingIntent,
                       wela.INTEREST_IN_SUSPENSE AS orgSuspendedInterest,
                       CASE
                           WHEN wela.CURRENCY = 'USD' THEN wela.INTEREST_IN_SUSPENSE
                           ELSE NULL
                           END AS usdSuspendedInterest,
                       CASE
                           WHEN wela.CURRENCY = 'USD' THEN wela.INTEREST_IN_SUSPENSE * 2500
                           ELSE wela.INTEREST_IN_SUSPENSE
                           END AS tzsSuspendedInterest
                from W_EOM_LOAN_ACCOUNT as wela
                         LEFT JOIN CUSTOMER as c ON wela.CUST_ID = c.CUST_ID
                         LEFT JOIN PROFITS_ACCOUNT pa ON pa.CUST_ID = wela.CUST_ID
                         LEFT JOIN other_id id ON (CASE WHEN (id.serial_no IS NULL) THEN '1' ELSE id.main_flag END = '1' AND id.fk_customercust_id = c.cust_id)
                         LEFT JOIN LNS_CRD_CUST_DATA lccd on lccd.CUST_ID = wela.CUST_ID
                         LEFT JOIN generic_detail id_country ON (id.fkgh_has_been_issu = id_country.fk_generic_headpar AND id.fkgd_has_been_issu = id_country.serial_num)
                         LEFT JOIN CUSTOMER_TYPES_LOOKUP ctl ON ctl.CUSTOMER_TYPE_CODE = c.CUST_TYPE
                         LEFT JOIN COUNTRIES_LOOKUP cl ON cl.COUNTRY_NAME = id_country.description
                         LEFT JOIN GENERIC_DETAIL GG ON GG.FK_GENERIC_HEADPAR = wela.FKGH_HAS_AS_LOAN_P AND GG.SERIAL_NUM = wela.FKGD_HAS_AS_LOAN_P
                         LEFT JOIN LOAN_ACCOUNT L ON wela.FK_UNITCODE = L.FK_UNITCODE AND wela.ACC_TYPE = L.ACC_TYPE AND wela.ACC_SN = L.ACC_SN
                         LEFT JOIN LOAN_ADD_INFO N ON N.ROW_ID = 1 AND wela.FK_UNITCODE = N.ACC_UNIT AND wela.ACC_TYPE = N.ACC_TYPE AND wela.ACC_SN = N.ACC_SN
                ORDER BY wela.ACC_OPEN_DT DESC
                FETCH FIRST 1000 ROWS ONLY
                """,
                timestamp_column='ACC_OPEN_DT',
                target_table='interBankLoanReceivable',
                queue_name='inter_bank_loan_receivable_queue',
                processor_class='InterBankLoanReceivableProcessor',
                batch_size=1000,
                poll_interval=10
            ),
            'icbmTransaction': TableConfig(
                name='icbmTransaction',
                query="""
                select
                    CURRENT_TIMESTAMP as reportingDate,
                    gte.TRN_DATE as transactionDate,
                    '' as lenderName,
                    '' as borrowerName,
                    CASE
                        WHEN TIME(gte.TMSTAMP) < TIME('15:30:00') THEN 'market'
                        ELSE 'off market'
                    END AS transactionType,
                    gte.DC_AMOUNT as tzsAmount,
                    '' as tenure,
                    '' as interestRate
                from
                    GLI_TRX_EXTRACT as gte
                    JOIN GLG_ACCOUNT as gl ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO
                WHERE
                    gl.EXTERNAL_GLACCOUNT = '102000001'
                ORDER BY gte.TRN_DATE DESC
                FETCH FIRST 1000 ROWS ONLY
                """,
                timestamp_column='TRN_DATE',
                target_table='icbmTransaction',
                queue_name='icbm_transaction_queue',
                processor_class='IcbmTransactionProcessor',
                batch_size=1000,
                poll_interval=10
            ),
            'investmentDebtSecurities': TableConfig(
                name='investmentDebtSecurities',
                query="""
                -- Investment Debt Securities for RTSIS Reporting - WORKING VERSION
                -- Using both GLI_TRX_EXTRACT (130% GL accounts) and DEPOSIT_ACCOUNT (specific DEPOSIT_TYPE values)
                -- Based on testing: 
                -- - GLI_TRX_EXTRACT: 4 GL accounts with 130% pattern (Treasury bonds)
                -- DEPOSIT_ACCOUNT: 51,657 records with DEPOSIT_TYPE 1-5 (mostly Corporate bonds)
                -- - Query performance optimized with subquery approach for GL accounts

                -- OPTION 1: Government Bonds from GL Transaction Extract
                SELECT
                    -- Fixed timestamp for reporting
                    CURRENT_TIMESTAMP AS reportingDate,
                    
                    -- Security identification (using GL account + customer combination)
                    (gte.FK_GLG_ACCOUNTACCO || '-' || COALESCE(CAST(gte.CUST_ID AS VARCHAR(10)), '0')) AS securityNumber,
                    
                    -- Security type classification - Treasury bonds (130% GL accounts)
                    'Treasury bonds' AS securityType,
                    
                    -- Issuer information - Government of Tanzania for 130% GL accounts
                    'Government of Tanzania' AS securityIssuerName,
                    
                    -- External rating - AAA for Government securities
                    'AAA' AS externalIssuerRatting,
                    
                    -- Grades for unrated banks - NULL for Government securities
                    NULL AS gradesUnratedBanks,
                    
                    -- Issuer country - Tanzania for Government securities
                    'Tanzania' AS securityIssuerCountry,
                    
                    -- SNA sector classification - Central Government
                    'Central Government' AS snaIssuerSector,
                    
                    -- Currency
                    COALESCE(gte.CURRENCY_SHORT_DES, 'TZS') AS currency,
                    
                    -- Cost value amounts (using DC_AMOUNT as cost basis)
                    gte.DC_AMOUNT AS orgCostValueAmount,
                    
                    -- TZS cost value
                    CASE 
                        WHEN gte.CURRENCY_SHORT_DES = 'USD' 
                            THEN gte.DC_AMOUNT * 2730.50
                        WHEN gte.CURRENCY_SHORT_DES = 'EUR'
                            THEN gte.DC_AMOUNT * 2950.00
                        ELSE gte.DC_AMOUNT
                    END AS tzsCostValueAmount,
                    
                    -- USD cost value
                    CASE 
                        WHEN gte.CURRENCY_SHORT_DES = 'USD' 
                            THEN gte.DC_AMOUNT
                        WHEN gte.CURRENCY_SHORT_DES = 'TZS'
                            THEN gte.DC_AMOUNT / 2730.50
                        WHEN gte.CURRENCY_SHORT_DES = 'EUR'
                            THEN gte.DC_AMOUNT * 1.08
                        ELSE NULL
                    END AS usdCostValueAmount,
                    
                    -- Face value amounts (same as cost for GL account approach)
                    gte.DC_AMOUNT AS orgFaceValueAmount,
                    
                    -- TZS face value
                    CASE 
                        WHEN gte.CURRENCY_SHORT_DES = 'USD' 
                            THEN gte.DC_AMOUNT * 2730.50
                        WHEN gte.CURRENCY_SHORT_DES = 'EUR'
                            THEN gte.DC_AMOUNT * 2950.00
                        ELSE gte.DC_AMOUNT
                    END AS tzsgFaceValueAmount,
                    
                    -- USD face value
                    CASE 
                        WHEN gte.CURRENCY_SHORT_DES = 'USD' 
                            THEN gte.DC_AMOUNT
                        WHEN gte.CURRENCY_SHORT_DES = 'TZS'
                            THEN gte.DC_AMOUNT / 2730.50
                        WHEN gte.CURRENCY_SHORT_DES = 'EUR'
                            THEN gte.DC_AMOUNT * 1.08
                        ELSE NULL
                    END AS usdgFaceValueAmount,
                    
                    -- Fair value amounts (same as cost for GL account approach)
                    gte.DC_AMOUNT AS orgFairValueAmount,
                    
                    -- TZS fair value
                    CASE 
                        WHEN gte.CURRENCY_SHORT_DES = 'USD' 
                            THEN gte.DC_AMOUNT * 2730.50
                        WHEN gte.CURRENCY_SHORT_DES = 'EUR'
                            THEN gte.DC_AMOUNT * 2950.00
                        ELSE gte.DC_AMOUNT
                    END AS tzsgFairValueAmount,
                    
                    -- USD fair value
                    CASE 
                        WHEN gte.CURRENCY_SHORT_DES = 'USD' 
                            THEN gte.DC_AMOUNT
                        WHEN gte.CURRENCY_SHORT_DES = 'TZS'
                            THEN gte.DC_AMOUNT / 2730.50
                        WHEN gte.CURRENCY_SHORT_DES = 'EUR'
                            THEN gte.DC_AMOUNT * 1.08
                        ELSE NULL
                    END AS usdgFairValueAmount,
                    
                    -- Interest rate (placeholder - no bond master data available)
                    CAST(0 AS DECIMAL(9, 6)) AS interestRate,
                    
                    -- Purchase date (using transaction date)
                    gte.TRN_DATE AS purchaseDate,
                    
                    -- Value date (using availability date)
                    gte.AVAILABILITY_DATE AS valueDate,
                    
                    -- Maturity date (using availability date as proxy)
                    gte.AVAILABILITY_DATE AS maturityDate,
                    
                    -- Trading intent - Government securities typically held to maturity
                    'Hold to Maturity' AS tradingIntent,
                    
                    -- Security encumbrance status (placeholder)
                    'Unencumbered' AS securityEncumbaranceStatus,
                    
                    -- Past due days (for securities past maturity)
                    CASE 
                        WHEN gte.AVAILABILITY_DATE IS NOT NULL AND gte.AVAILABILITY_DATE < CURRENT_DATE
                            THEN DAYS(CURRENT_DATE) - DAYS(gte.AVAILABILITY_DATE)
                        ELSE 0
                    END AS pastDueDays,
                    
                    -- Allowance for probable loss (placeholder)
                    CAST(0 AS DECIMAL(15, 2)) AS allowanceProbableLoss,
                    
                    -- Asset classification category
                    CASE 
                        WHEN gte.AVAILABILITY_DATE IS NULL OR gte.AVAILABILITY_DATE >= CURRENT_DATE
                            THEN 1  -- Normal
                        WHEN DAYS(CURRENT_DATE) - DAYS(gte.AVAILABILITY_DATE) <= 90
                            THEN 2  -- Watch
                        WHEN DAYS(CURRENT_DATE) - DAYS(gte.AVAILABILITY_DATE) <= 180
                            THEN 3  -- Substandard
                        WHEN DAYS(CURRENT_DATE) - DAYS(gte.AVAILABILITY_DATE) <= 365
                            THEN 4  -- Doubtful
                        ELSE 5  -- Loss
                    END AS assetClassificationCategory

                FROM GLI_TRX_EXTRACT gte

                -- Join with GL account for account details
                LEFT JOIN GLG_ACCOUNT gl 
                    ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID

                -- Join with customer information
                LEFT JOIN CUSTOMER c 
                    ON gte.CUST_ID = c.CUST_ID

                WHERE 
                    -- Only Government Bond GL accounts (130% pattern - optimized with subquery)
                    gte.FK_GLG_ACCOUNTACCO IN (
                        SELECT ACCOUNT_ID 
                        FROM GLG_ACCOUNT 
                        WHERE EXTERNAL_GLACCOUNT LIKE '130%'
                    )
                    -- Only debit balances (assets)
                    AND gte.DC_AMOUNT IS NOT NULL
                    AND gte.DC_AMOUNT > 0
                    -- Only active records
                    AND gte.TRN_DATE IS NOT NULL
                    AND gte.TRN_DATE >= '2024-01-01'

                UNION ALL

                -- OPTION 2: Investment Securities from Deposit Accounts (DEPOSIT_TYPE 2, 4, 5)
                SELECT
                    -- Fixed timestamp for reporting
                    CURRENT_TIMESTAMP AS reportingDate,
                    
                    -- Security identification (using account number)
                    CAST(da.ACCOUNT_NUMBER AS VARCHAR(50)) AS securityNumber,
                    
                    -- Security type classification based on deposit type (mapped to RTSIS codes)
                    CASE 
                        WHEN da.DEPOSIT_TYPE = '1' THEN 'Corporate bonds'           -- Code 1
                        WHEN da.DEPOSIT_TYPE = '2' THEN 'Treasury bonds'            -- Code 2  
                        WHEN da.DEPOSIT_TYPE = '3' THEN 'Treasury bills'            -- Code 3
                        WHEN da.DEPOSIT_TYPE = '4' THEN 'RGOZ Treasury bond'        -- Code 4
                        WHEN da.DEPOSIT_TYPE = '5' THEN 'Municipal/Local Government bond'  -- Code 5
                        ELSE 'Others investments (Specify name)'                   -- Code 16
                    END AS securityType,
                    
                    -- Issuer information based on deposit type
                    CASE 
                        WHEN da.DEPOSIT_TYPE = '2' THEN 'Government of Tanzania'     -- Treasury bonds
                        WHEN da.DEPOSIT_TYPE = '3' THEN 'Bank of Tanzania'           -- Treasury bills
                        WHEN da.DEPOSIT_TYPE = '4' THEN 'Government of Tanzania'     -- RGOZ Treasury bond
                        WHEN da.DEPOSIT_TYPE = '5' THEN 'Local Government Authority' -- Municipal bonds
                        WHEN da.DEPOSIT_TYPE = '1' AND c.CUST_ID IS NOT NULL AND c.CUST_TYPE = 'C'
                            THEN TRIM(COALESCE(c.FIRST_NAME, '') || ' ' || COALESCE(c.MIDDLE_NAME, '') || ' ' || COALESCE(c.SURNAME, ''))
                        WHEN da.DEPOSIT_TYPE = '1' AND c.CUST_ID IS NOT NULL
                            THEN COALESCE(c.SURNAME, 'Unknown Corporate')
                        ELSE 'Unknown Issuer'
                    END AS securityIssuerName,
                    
                    -- External rating based on deposit type
                    CASE 
                        WHEN da.DEPOSIT_TYPE IN ('2', '3', '4') THEN 'AAA'  -- Government/Treasury securities
                        WHEN da.DEPOSIT_TYPE = '5' THEN 'A'                 -- Municipal bonds
                        WHEN UPPER(COALESCE(c.SURNAME, '')) LIKE '%BANK%' THEN 'A'
                        ELSE NULL
                    END AS externalIssuerRatting,
                    
                    -- Grades for unrated banks
                    CASE 
                        WHEN UPPER(COALESCE(c.SURNAME, '')) LIKE '%BANK%' 
                             AND da.DEPOSIT_TYPE = '1'  -- Only for corporate bonds
                        THEN 'Grade 2'
                        ELSE NULL
                    END AS gradesUnratedBanks,
                    
                    -- Issuer country
                    'Tanzania' AS securityIssuerCountry,
                    
                    -- SNA sector classification
                    CASE 
                        WHEN da.DEPOSIT_TYPE IN ('2', '3', '4') THEN 'Central Government'  -- Government securities
                        WHEN da.DEPOSIT_TYPE = '5' THEN 'Local Government'                 -- Municipal bonds
                        WHEN da.DEPOSIT_TYPE = '1' AND UPPER(COALESCE(c.SURNAME, '')) LIKE '%BANK%'
                            THEN 'Other Depository Corporations'
                        WHEN da.DEPOSIT_TYPE = '1' AND UPPER(COALESCE(c.SURNAME, '')) LIKE '%INSURANCE%'
                            THEN 'Insurance Companies'
                        WHEN da.DEPOSIT_TYPE = '1' AND c.CUST_ID IS NOT NULL AND c.CUST_TYPE = 'C'
                            THEN 'Households'
                        ELSE 'Other Non-Financial Corporations'
                    END AS snaIssuerSector,
                    
                    -- Currency
                    COALESCE(cur.SHORT_DESCR, 'TZS') AS currency,
                    
                    -- Cost value amounts (using opening balance as cost basis)
                    COALESCE(da.OPENING_BALANCE, da.BOOK_BALANCE, 0) AS orgCostValueAmount,
                    
                    -- TZS cost value
                    CASE 
                        WHEN cur.SHORT_DESCR = 'USD' 
                            THEN COALESCE(da.OPENING_BALANCE, da.BOOK_BALANCE, 0) * 2730.50
                        WHEN cur.SHORT_DESCR = 'EUR'
                            THEN COALESCE(da.OPENING_BALANCE, da.BOOK_BALANCE, 0) * 2950.00
                        ELSE COALESCE(da.OPENING_BALANCE, da.BOOK_BALANCE, 0)
                    END AS tzsCostValueAmount,
                    
                    -- USD cost value
                    CASE 
                        WHEN cur.SHORT_DESCR = 'USD' 
                            THEN COALESCE(da.OPENING_BALANCE, da.BOOK_BALANCE, 0)
                        WHEN cur.SHORT_DESCR = 'TZS'
                            THEN COALESCE(da.OPENING_BALANCE, da.BOOK_BALANCE, 0) / 2730.50
                        WHEN cur.SHORT_DESCR = 'EUR'
                            THEN COALESCE(da.OPENING_BALANCE, da.BOOK_BALANCE, 0) * 1.08
                        ELSE NULL
                    END AS usdCostValueAmount,
                    
                    -- Face value amounts (using book balance as face value)
                    COALESCE(da.BOOK_BALANCE, 0) AS orgFaceValueAmount,
                    
                    -- TZS face value
                    CASE 
                        WHEN cur.SHORT_DESCR = 'USD' 
                            THEN COALESCE(da.BOOK_BALANCE, 0) * 2730.50
                        WHEN cur.SHORT_DESCR = 'EUR'
                            THEN COALESCE(da.BOOK_BALANCE, 0) * 2950.00
                        ELSE COALESCE(da.BOOK_BALANCE, 0)
                    END AS tzsgFaceValueAmount,
                    
                    -- USD face value
                    CASE 
                        WHEN cur.SHORT_DESCR = 'USD' 
                            THEN COALESCE(da.BOOK_BALANCE, 0)
                        WHEN cur.SHORT_DESCR = 'TZS'
                            THEN COALESCE(da.BOOK_BALANCE, 0) / 2730.50
                        WHEN cur.SHORT_DESCR = 'EUR'
                            THEN COALESCE(da.BOOK_BALANCE, 0) * 1.08
                        ELSE NULL
                    END AS usdgFaceValueAmount,
                    
                    -- Fair value amounts (using available balance as fair value)
                    COALESCE(da.AVAILABLE_BALANCE, da.BOOK_BALANCE, 0) AS orgFairValueAmount,
                    
                    -- TZS fair value
                    CASE 
                        WHEN cur.SHORT_DESCR = 'USD' 
                            THEN COALESCE(da.AVAILABLE_BALANCE, da.BOOK_BALANCE, 0) * 2730.50
                        WHEN cur.SHORT_DESCR = 'EUR'
                            THEN COALESCE(da.AVAILABLE_BALANCE, da.BOOK_BALANCE, 0) * 2950.00
                        ELSE COALESCE(da.AVAILABLE_BALANCE, da.BOOK_BALANCE, 0)
                    END AS tzsgFairValueAmount,
                    
                    -- USD fair value
                    CASE 
                        WHEN cur.SHORT_DESCR = 'USD' 
                            THEN COALESCE(da.AVAILABLE_BALANCE, da.BOOK_BALANCE, 0)
                        WHEN cur.SHORT_DESCR = 'TZS'
                            THEN COALESCE(da.AVAILABLE_BALANCE, da.BOOK_BALANCE, 0) / 2730.50
                        WHEN cur.SHORT_DESCR = 'EUR'
                            THEN COALESCE(da.AVAILABLE_BALANCE, da.BOOK_BALANCE, 0) * 1.08
                        ELSE NULL
                    END AS usdgFairValueAmount,
                    
                    -- Interest rate (using fixed interest rate from deposit account)
                    COALESCE(da.FIXED_INTER_RATE, 0) AS interestRate, 
                    
                    -- Purchase date (using opening date)
                    da.OPENING_DATE AS purchaseDate,
                    
                    -- Value date (using start date)
                    COALESCE(da.START_DATE_TD, da.OPENING_DATE) AS valueDate,
                    
                    -- Maturity date (using expiry date)
                    COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE) AS maturityDate,
                    
                    -- Trading intent based on deposit type
                    CASE 
                        WHEN da.DEPOSIT_TYPE IN ('2', '3', '4') THEN 'Hold to Maturity'  -- Government securities
                        WHEN da.DEPOSIT_TYPE = '5' THEN 'Hold to Maturity'              -- Municipal bonds
                        ELSE 'Available for Sale'  -- Corporate bonds
                    END AS tradingIntent,
                    
                    -- Security encumbrance status based on collateral flag (using numeric values)
                    CASE 
                        WHEN da.COLLATERAL_FLG = '1' THEN 'Encumbered'
                        ELSE 'Unencumbered'
                    END AS securityEncumbaranceStatus,
                    
                    -- Past due days (for securities past maturity)
                    CASE 
                        WHEN COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE) IS NOT NULL 
                             AND COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE) < CURRENT_DATE
                            THEN DAYS(CURRENT_DATE) - DAYS(COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE))
                        ELSE 0
                    END AS pastDueDays,
                    
                    -- Allowance for probable loss (placeholder)
                    CAST(0 AS DECIMAL(15, 2)) AS allowanceProbableLoss,
                    
                    -- Asset classification category based on maturity and status
                    CASE 
                        WHEN da.ENTRY_STATUS NOT IN ('1', '6') THEN 5  -- Loss if not active
                        WHEN COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE) IS NULL 
                             OR COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE) >= CURRENT_DATE
                            THEN 1  -- Normal
                        WHEN DAYS(CURRENT_DATE) - DAYS(COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE)) <= 90
                            THEN 2  -- Watch
                        WHEN DAYS(CURRENT_DATE) - DAYS(COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE)) <= 180
                            THEN 3  -- Substandard
                        WHEN DAYS(CURRENT_DATE) - DAYS(COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE)) <= 365
                            THEN 4  -- Doubtful
                        ELSE 5  -- Loss
                    END AS assetClassificationCategory

                FROM DEPOSIT_ACCOUNT da

                -- Join with customer information
                LEFT JOIN CUSTOMER c 
                    ON da.FK_CUSTOMERCUST_ID = c.CUST_ID

                -- Join with currency information
                LEFT JOIN CURRENCY cur 
                    ON da.FK_CURRENCYID_CURR = cur.ID_CURRENCY

                WHERE 
                    -- Only investment securities (DEPOSIT_TYPE 1-5 for debt securities)
                    da.DEPOSIT_TYPE IN ('1', '2', '3', '4', '5')
                    -- Only active accounts (status 1 and 6 appear to be active based on counts)
                    AND da.ENTRY_STATUS IN ('1', '6')
                    -- Only accounts with balances
                    AND (da.BOOK_BALANCE > 0 OR da.AVAILABLE_BALANCE > 0 OR da.OPENING_BALANCE > 0)

                ORDER BY 
                    securityType, securityNumber
                FETCH FIRST 1000 ROWS ONLY
                """,
                timestamp_column='reportingDate',
                target_table='investmentDebtSecurities',
                queue_name='investment_debt_securities_queue',
                processor_class='InvestmentDebtSecuritiesProcessor',
                batch_size=1000,
                poll_interval=10
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