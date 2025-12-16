create table W_EOM_LOAN_ACCOUNT
(
    EOM_DATE                      DATE                      not null,
    ACCOUNT_NUMBER                CHAR(40)                  not null,
    C_DIGIT                       SMALLINT,
    NRM_RL_URL_FLG                SMALLINT,
    OV_RL_URL_FLG                 SMALLINT,
    ACCOUNT_CD                    SMALLINT,
    PRFT_SYSTEM                   SMALLINT,
    AGREEMENT_CD                  SMALLINT,
    AGREEMENT_SYSTEM              SMALLINT,
    ACC_TYPE                      SMALLINT,
    INSTALL_FREQ                  SMALLINT,
    INSTALL_COUNT                 SMALLINT,
    LAST_NRM_TRX_CNT              SMALLINT,
    FK_BASE_RATEFK_GD             INTEGER,
    FK_UNITCODE                   INTEGER,
    LNS_OPEN_UNIT                 INTEGER,
    ID_PRODUCT                    INTEGER,
    FKCUR_IS_CHARGED              INTEGER,
    FKCUR_IS_MOVED_IN             INTEGER,
    FKCUR_USES_AS_LIM             INTEGER,
    FKGD_HAS_AS_CLASS             INTEGER,
    FKGD_HAS_AS_FINANC            INTEGER,
    FKGD_HAS_AS_LOAN_P            INTEGER,
    FKGD_CATEGORY                 INTEGER,
    FK_INTERESTID_INTE            INTEGER,
    ACC_SN                        INTEGER,
    CUST_ID                       INTEGER,
    REMAINING_DAYS                DECIMAL(10),
    REMAINING_MONTHS              DECIMAL(10),
    TOTAL_DAYS                    DECIMAL(10),
    TOTAL_MONTHS                  DECIMAL(10),
    OVERDUE_DAYS                  DECIMAL(10),
    OVERDUE_MONTHS                DECIMAL(10),
    NRM_CAP_BAL                   DECIMAL(15, 2),
    NRM_RL_INT_BAL                DECIMAL(15, 2),
    NRM_URL_INT_BAL               DECIMAL(15, 2),
    NRM_EXP_BAL                   DECIMAL(15, 2),
    NRM_COM_BAL                   DECIMAL(15, 2),
    NRM_ACR_INT_BAL               DECIMAL(15, 2),
    NRM_SUBSIDY_BAL               DECIMAL(15, 2),
    OV_CAP_BAL                    DECIMAL(15, 2),
    OV_RL_NRM_INT_BAL             DECIMAL(15, 2),
    OV_RL_PNL_INT_BAL             DECIMAL(15, 2),
    OV_URL_NRM_INT_BAL            DECIMAL(15, 2),
    OV_URL_PNL_INT_BAL            DECIMAL(15, 2),
    OV_EXP_BAL                    DECIMAL(15, 2),
    OV_COM_BAL                    DECIMAL(15, 2),
    OV_ACR_NRM_INT_BAL            DECIMAL(15, 2),
    OV_ACR_PNL_INT_BAL            DECIMAL(15, 2),
    OV_SUBSIDY_BAL                DECIMAL(15, 2),
    ACC_LIMIT_AMN                 DECIMAL(15, 2),
    POSITIVE_AMN                  DECIMAL(15, 2),
    UNCLEAR_AMN                   DECIMAL(15, 2),
    BLOCKED_AMN                   DECIMAL(15, 2),
    DORMANT_AMN                   DECIMAL(15, 2),
    INSTALL_FIXED_AMN             DECIMAL(15, 2),
    NRM_ACCRUAL_AMN               DECIMAL(15, 2),
    OV_ACCRUAL_AMN                DECIMAL(15, 2),
    LNS_UNREALIZED                DECIMAL(15, 2),
    PRFT_ACC_LMT_AMN              DECIMAL(15, 2),
    BOOK_BALANCE                  DECIMAL(15, 2),
    NRM_BALANCE                   DECIMAL(15, 2),
    OV_BALANCE                    DECIMAL(15, 2),
    OV_MKT                        DECIMAL(15, 2),
    SELECTED_BANK_RATE            DECIMAL(8, 4),
    SELECTED_NORMAL_RA            DECIMAL(8, 4),
    FINAL_INTEREST                DECIMAL(8, 4),
    BASE_SPREAD                   DECIMAL(8, 4),
    SPREAD                        DECIMAL(8, 4),
    N128                          DECIMAL(9, 6),
    BANK_SPREAD                   DECIMAL(12, 6),
    INSTALL_FIRST_DT              DATE,
    INSTALL_NEXT_DT               DATE,
    INSTALL_PREV_DT               DATE,
    ACC_EXP_DT                    DATE,
    ACC_OPEN_DT                   DATE,
    OV_EXP_DT                     DATE,
    LOAN_STATUS                   CHAR(1),
    ACC_STATUS                    CHAR(1),
    ACC_MECHANISM                 CHAR(1),
    FIRST_NAME                    CHAR(20),
    NRM_DR_CNTR_GL_ACC            CHAR(21),
    NRM_CR_CNTR_GL_ACC            CHAR(21),
    OV_DR_CNTR_GL_ACC             CHAR(21),
    DEFINATE_DELAY                CHAR(21),
    WRITE_OFF                     CHAR(21),
    AGREEMENT_NUMBER              CHAR(40),
    SURNAME                       CHAR(70),
    PAN_DUE_CAT                   VARCHAR(8),
    LNS_INTEREST_DESC             VARCHAR(40),
    DESCRIPTION                   VARCHAR(40),
    PRODUCT_DESC                  VARCHAR(40),
    UNIT_NAME                     VARCHAR(40),
    DRAWDOWN_FST_AMN              DECIMAL(15, 2),
    DRAWDOWN_FST_DT               DATE,
    INSTA_PAID                    INTEGER,
    PROVISION_AMN                 DECIMAL(15, 2),
    PROVISION_CURR_PERC           INTEGER,
    TOT_CAP_AMN                   DECIMAL(15, 2),
    TOT_COMMISSION_AMN            DECIMAL(15, 2),
    TOT_DRAWDOWN_AMN              DECIMAL(15, 2),
    TOT_EXPENSE_AMN               DECIMAL(15, 2),
    TOT_NRM_INT_AMN               DECIMAL(15, 2),
    TOT_PNL_INT_AMN               DECIMAL(15, 2),
    FKGH_CBPURP                   CHAR(5),
    FKGD_CBPURP                   INTEGER,
    FKINT_CURRENT_FIX             INTEGER,
    FKINT_AS_FLOATING             INTEGER,
    FKINT_PREV_FIXED              INTEGER,
    DEL_FKINT_HAS_OVERDUE         INTEGER,
    PENALTY_INTE_RATE             DECIMAL(8, 4),
    CUST_TYPE                     SMALLINT,
    OV_GL_ACCRUAL_ACC             CHAR(21),
    DEF_GL_ACCRUAL_ACC            CHAR(21),
    DEL_CUR_FX_INT_EXP_DT         DATE,
    DEL_CUR_FX_INT_ST_DT          DATE,
    LOAN_TYPE                     CHAR(2),
    PREV_SUB_CLASS                CHAR(1),
    CURR_SUB_CLASS                CHAR(1),
    DEL_AFM_NO                    CHAR(20),
    DEL_ID_NO                     VARCHAR(20),
    DEL_DESCRIPTION_ID            VARCHAR(40),
    PROVISION_AMOUNT              DECIMAL(15, 2),
    FKGH_HAS_AS_CLASS             CHAR(5),
    EURO_BOOK_BAL                 DECIMAL(15, 2),
    FIXING_RATE                   DECIMAL(12, 6),
    TRX_AMN_WO_WD                 DECIMAL(15, 2),
    TRX_AMN_CAP_INT               DECIMAL(15, 2),
    DISCOUNTED_VALUE              DECIMAL(15, 2),
    WRITE_OFF_PAY_AMT             DECIMAL(15, 2),
    FKGH_HAS_AS_FINANC            CHAR(5),
    FKGH_AS_CRED_LINE             CHAR(5),
    FKGD_AS_CRED_LINE             INTEGER,
    DEL_JUSTIFICATION_CODE        INTEGER,
    DEL_TRANSACTION_CODE          INTEGER,
    TRX_AMN                       DECIMAL(15, 2),
    DEL_REVERSED_FLG              CHAR(1),
    REQUEST_TYPE                  CHAR(1),
    DRAWDOWN_EXP_DT               DATE,
    DEL_HOLDR_FK_GENERIC_DETASER  INTEGER,
    HOLDR_DESCRIPTION             CHAR(20),
    FKGH_CATEGORY                 CHAR(5),
    FK_GENERIC_DETASER            INTEGER,
    FK_CATEGORYCATEGOR            CHAR(5),
    LAST_TRANSACTION_DATE         DATE,
    DEL_EMP_FIRST_NAME            VARCHAR(40),
    DEL_EMP_LAST_NAME             VARCHAR(70),
    DEL_NRM_CHRG_CNT              SMALLINT,
    DEL_NRM_INST_CNT              SMALLINT,
    DEL_NRM_INT_CNT               SMALLINT,
    DEL_NRM_LOAN_CNT              SMALLINT,
    OV_INST_CNT                   SMALLINT,
    DEL_OV_INT_CNT                SMALLINT,
    DEL_OV_LOAN_CNT               SMALLINT,
    DEL_OV_CHRG_CNT               SMALLINT,
    DEL_TRX_DATE                  DATE,
    RECYCLING_LMT_FLG             CHAR(1),
    LUMP_DRAWDOWN_FLG             CHAR(1),
    MONOTORING_UNIT               INTEGER,
    LOAN_CLASS                    CHAR(1),
    AGR_LIMIT                     DECIMAL(15, 2),
    GROSS_TOTAL                   DECIMAL(15, 2),
    LC_GROSS_TOTAL                DECIMAL(15, 2) default 0,
    INTEREST_IN_SUSPENSE          DECIMAL(15, 2) default 0,
    DEL_FINAL_CLASS               CHAR(1),
    ADJUSTED_SUB_CLASS            CHAR(1),
    ACTUAL_SUB_CLASS              CHAR(1),
    ADJUSTED_CLASS                CHAR(1),
    COLLATERAL_OM_VALUE           DECIMAL(15, 2),
    FINAL_SUB_CLASS               CHAR(1),
    ACC_DRAWDOWN_STS              CHAR(1),
    ACCT_KEY                      DECIMAL(11),
    FKGH_HAS_ADJUST               CHAR(5),
    FKGD_HAS_ADJUST               INTEGER,
    CURRENCY                      CHAR(5),
    ISS_REMAINING_AMNT            DECIMAL(15, 2),
    SERVICE_DEP_ACCT_KEY          DECIMAL(11),
    AGREEM_ACCT_KEY               DECIMAL(11),
    CLOSED_FLAG                   CHAR(3)        default 'No',
    TOT_INT_SPRD_AMN              DECIMAL(15, 2),
    TOT_SUBS_INT_AMN              DECIMAL(15, 2),
    CUSTOMER_NAME                 VARCHAR(300),
    LOAN_TYPE_NAME                VARCHAR(38),
    LAST_MONTH_INTEREST_DEBIT_AMT DECIMAL(15, 2),
    FINAL_CLASS_NAME              VARCHAR(14),
    DATE_CLASS_CHANGED            DATE,
    ACCOUNT_NO                    VARCHAR(50),
    CRM_ARREARS_DATE              DATE,
    CRM_LAND_REGISTRY_NUMBER      VARCHAR(40),
    CRM_NOTICE_ISSUE_DATE_90      DATE,
    CRM_NOTICE_MATURITY_DATE_90   DATE,
    CRM_NOTICE_ISSUE_DATE_40      DATE,
    CRM_NOTICE_MATURITY_DATE_40   DATE,
    CRM_AUCTION_DATE              DATE,
    CRM_COMMENTS                  VARCHAR(500),
    RECOVERIES_DATE               DATE,
    MONTHLY_PREMIUM               DECIMAL(15, 2) default 0,
    LEDGER_FEES                   DECIMAL(15, 2) default 0,
    DELAY_OFFICER_ID              VARCHAR(8),
    DELAY_OFFICER_NAME            VARCHAR(41),
    LOAN_STATUS_IND_NAME          VARCHAR(15),
    CLOAN_CATEGORY_DESCRIPTION    VARCHAR(40),
    AGREEMENT_NUMERIC             CHAR(40),
    CRM_LAND_REGISTRY_NUMERIC     VARCHAR(40),
    LOAN_OFFICER_ID               VARCHAR(8),
    LOAN_OFFICER_NAME             VARCHAR(41),
    INSTALLMENT_AMOUNT            DECIMAL(15, 2),
    REMAINING_CAPITAL_AMT         DECIMAL(15, 2),
    TRANSFER_WRITEOFF_DATE        DATE,
    CRM_AMOUNT_PAID               DECIMAL(15, 2),
    CRM_SALE_PRICE                DECIMAL(15, 2),
    CRM_SALE_DATE                 DATE,
    CRM_COMPLETION_DATE           DATE,
    CRM_AUCTIONEER                VARCHAR(60),
    TOTAL_CAPITAL_DEBITED         DECIMAL(15, 2),
    TOTAL_INTEREST_CREDITED       DECIMAL(15, 2),
    SO_FIRST_PRIORITY_ACCOUNT_NO  VARCHAR(50),
    SO_SECOND_PRIORITY_ACCOUNT_NO VARCHAR(50),
    FINANCIAL_SECTOR              VARCHAR(40),
    SALESPERSON                   VARCHAR(40),
    OVERDRAFT_TYPE_FLAG           VARCHAR(13),
    INSURANCES_AMOUNT             DECIMAL(15, 2),
    DAILY_DEBIT                   DECIMAL(15, 2),
    DAILY_CREDIT                  DECIMAL(15, 2),
    MONITORING_UNIT_NAME          VARCHAR(40),
    MEDIATOR_CODE                 VARCHAR(25)    default '' not null,
    MEDIATOR_DESCRIPTION          VARCHAR(25),
    DEF_DELAY_DATE                DATE,
    DEF_DELAY_AMN                 DECIMAL(15, 2),
    WRITE_OFF_DATE                DATE,
    WRITE_OFF_AMN                 DECIMAL(15, 2),
    LAST_PAYMENT_DATE             DATE,
    LAST_PAYMENT_AMN              DECIMAL(15, 2),
    ACCOUNTING_AMN                DECIMAL(15, 2),
    ADJUSTMENT_DT                 DATE,
    AGREEMENT_SIGNING_DT          DATE,
    PURCHACE_DT                   DATE,
    PURCHACE_AMN                  DECIMAL(15, 2),
    WRITE_OFF_RECOVERY_AMN        DECIMAL(15, 2),
    PRODUCT_CATEGORY              CHAR(50),
    WRITE_OFF_REASON              CHAR(50),
    DEF_DELAY_BUCKET              CHAR(50),
    DEF_DELAY_OV_DAYS             DECIMAL(10),
    FKGH_HAS_AS_LOAN_P            CHAR(5),
    NET_INT_LESS_60               DECIMAL(15, 2),
    NET_INT_LESS_90               DECIMAL(15, 2),
    BOOK_BALANCE_LESS_60          DECIMAL(15, 2),
    BOOK_BALANCE_LESS_90          DECIMAL(15, 2),
    REPORTING_BALANCE_LESS_90     DECIMAL(15, 2),
    constraint IXU_EOM_007
        primary key (EOM_DATE, ACCOUNT_NUMBER)
);

create unique index IDX_EOM_EOM_LOANS_ACCTKEY
    on W_EOM_LOAN_ACCOUNT (EOM_DATE, ACCT_KEY);

create unique index IXN_EOM_001
    on W_EOM_LOAN_ACCOUNT (CUST_ID);

CREATE PROCEDURE W_EOM_LOAN_ACCOUNT ( )
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
DELETE W_EOM_LOAN_ACCOUNT
 WHERE EOM_DATE = (SELECT SCHEDULED_DATE FROM BANK_PARAMETERS);
INSERT INTO W_EOM_LOAN_ACCOUNT (LNS_OPEN_UNIT,               UNIT_NAME,                 ACCOUNT_NUMBER,           ACCOUNT_CD,                    PRFT_SYSTEM,
                                AGREEMENT_NUMBER,            AGREEMENT_CD,              AGREEMENT_SYSTEM,         AGR_LIMIT,                     CUST_ID,
                                C_DIGIT,                     SURNAME,                   FIRST_NAME,               ID_PRODUCT,                    PRODUCT_DESC,
                                TOT_DRAWDOWN_AMN,            TOT_COMMISSION_AMN,        TOT_PNL_INT_AMN,          TOT_EXPENSE_AMN,               TOT_NRM_INT_AMN,
                                TOT_CAP_AMN,                 PROVISION_CURR_PERC,       NRM_CAP_BAL,              NRM_RL_INT_BAL,                NRM_URL_INT_BAL,
                                NRM_EXP_BAL,                 NRM_COM_BAL,               NRM_ACR_INT_BAL,          NRM_SUBSIDY_BAL,               OV_CAP_BAL,
                                OV_RL_NRM_INT_BAL,           OV_RL_PNL_INT_BAL,         OV_URL_NRM_INT_BAL,       OV_URL_PNL_INT_BAL,            OV_EXP_BAL,
                                OV_COM_BAL,                  OV_ACR_NRM_INT_BAL,        OV_ACR_PNL_INT_BAL,       OV_SUBSIDY_BAL,                INSTALL_FREQ,
                                INSTALL_COUNT,               INSTA_PAID,                INSTALL_FIRST_DT,         INSTALL_NEXT_DT,               INSTALL_PREV_DT,
                                ACC_EXP_DT,                  ACC_LIMIT_AMN,             ACC_OPEN_DT,              BOOK_BALANCE,                  EURO_BOOK_BAL,
                                NRM_BALANCE,                 OV_BALANCE,                NRM_RL_URL_FLG,           OV_RL_URL_FLG,                 FINAL_INTEREST,
                                FK_INTERESTID_INTE,          LNS_INTEREST_DESC,         BANK_SPREAD,              FK_BASE_RATEFK_GD,             DESCRIPTION,
                                BASE_SPREAD,                 SPREAD,                    N128,                     OV_MKT,                        POSITIVE_AMN,
                                UNCLEAR_AMN,                 BLOCKED_AMN,               DORMANT_AMN,              SELECTED_BANK_RATE,            SELECTED_NORMAL_RA,
                                INSTALL_FIXED_AMN,           NRM_ACCRUAL_AMN,           OV_ACCRUAL_AMN,           LNS_UNREALIZED,                PRFT_ACC_LMT_AMN,
                                NRM_DR_CNTR_GL_ACC,          NRM_CR_CNTR_GL_ACC,        OV_DR_CNTR_GL_ACC,        DEFINATE_DELAY,                WRITE_OFF,
                                MEDIATOR_CODE,               MEDIATOR_DESCRIPTION,      LOAN_STATUS,              ACC_STATUS,                    ACC_SN,
                                ACC_TYPE,                    FK_UNITCODE,               LAST_NRM_TRX_CNT,         FKCUR_IS_CHARGED,              FKCUR_IS_MOVED_IN,
                                FKCUR_USES_AS_LIM,           FKGD_HAS_AS_CLASS,         FKGH_HAS_AS_FINANC,       FKGD_HAS_AS_FINANC,            FKGD_HAS_AS_LOAN_P,
                                FKGH_CATEGORY,               FKGD_CATEGORY,             EOM_DATE,                 REMAINING_DAYS,                REMAINING_MONTHS,
                                TOTAL_DAYS,                  TOTAL_MONTHS,              OVERDUE_DAYS,             OVERDUE_MONTHS,                ACC_MECHANISM,
                                OV_EXP_DT,                   DRAWDOWN_FST_AMN,          DRAWDOWN_FST_DT,          PROVISION_AMN,                 FKGH_CBPURP,
                                FKGD_CBPURP,                 FKINT_CURRENT_FIX,         FKINT_AS_FLOATING,        FKINT_PREV_FIXED,              PENALTY_INTE_RATE,
                                CUST_TYPE,                   OV_GL_ACCRUAL_ACC,         DEF_GL_ACCRUAL_ACC,       LOAN_TYPE,                     LOAN_TYPE_NAME,
                                CURR_SUB_CLASS,              PROVISION_AMOUNT,          INTEREST_IN_SUSPENSE,     DISCOUNTED_VALUE,              FKGH_AS_CRED_LINE,
                                FKGD_AS_CRED_LINE,           DRAWDOWN_EXP_DT,           HOLDR_DESCRIPTION,        LAST_TRANSACTION_DATE,         MONOTORING_UNIT,
                                LOAN_CLASS,                  GROSS_TOTAL,               ACC_DRAWDOWN_STS,         FIXING_RATE,                   CURRENCY,
                                ACCT_KEY,                    AGREEM_ACCT_KEY,           SERVICE_DEP_ACCT_KEY,     CLOSED_FLAG,                   TOT_INT_SPRD_AMN,
                                TOT_SUBS_INT_AMN,            OV_INST_CNT,               CUSTOMER_NAME,            LAST_MONTH_INTEREST_DEBIT_AMT, ACCOUNT_NO,
                                CRM_ARREARS_DATE,            CRM_LAND_REGISTRY_NUMBER,  CRM_NOTICE_ISSUE_DATE_90, CRM_NOTICE_MATURITY_DATE_90,   CRM_NOTICE_ISSUE_DATE_40,
                                CRM_NOTICE_MATURITY_DATE_40, CRM_AUCTION_DATE,          CRM_COMMENTS,             RECOVERIES_DATE,               MONTHLY_PREMIUM,
                                LEDGER_FEES,                 DELAY_OFFICER_ID,          DELAY_OFFICER_NAME,       FKGH_HAS_AS_CLASS,             LOAN_STATUS_IND_NAME,
                                CLOAN_CATEGORY_DESCRIPTION,  INSTALLMENT_AMOUNT,        LOAN_OFFICER_ID,          LOAN_OFFICER_NAME,             CRM_AMOUNT_PAID,
                                CRM_SALE_PRICE,              CRM_SALE_DATE,             CRM_COMPLETION_DATE,      CRM_AUCTIONEER,                INSURANCES_AMOUNT,
                                OVERDRAFT_TYPE_FLAG,         SALESPERSON,               FINANCIAL_SECTOR,         SO_FIRST_PRIORITY_ACCOUNT_NO,  SO_SECOND_PRIORITY_ACCOUNT_NO,
                                TOTAL_CAPITAL_DEBITED,       TOTAL_INTEREST_CREDITED,   MONITORING_UNIT_NAME,     FKGH_HAS_AS_LOAN_P)
   WITH --SCALE0
        --AS (SELECT FK_INTERESTID_INTE,
        --           MAX (VALIDITY_DATE) AS MAXVALDATE
        --      FROM INT_SCALE, BANK_PARAMETERS
        --     WHERE SCHEDULED_DATE >= INT_SCALE.VALIDITY_DATE
        --    GROUP BY FK_INTERESTID_INTE),
        N128
        AS (SELECT SC0.PERCENTAGE,
                   SC0.FK_INTERESTID_INTE,
                   SC0.FK_CURRENCYID_CURR,
                   SC0.VALIDITY_DATE
              FROM INT_SCALE SC0
             WHERE (VALIDITY_DATE, FK_INTERESTID_INTE, FK_CURRENCYID_CURR) IN (SELECT MAX (VALIDITY_DATE) AS MAXVALIDITY_DATE,
                                                                                      FK_INTERESTID_INTE,
                                                                                      FK_CURRENCYID_CURR
                                                                                 FROM INT_SCALE,
                                                                                      BANK_PARAMETERS
                                                                                WHERE VALIDITY_DATE <= SCHEDULED_DATE
                                                                               GROUP BY FK_INTERESTID_INTE,
                                                                                        FK_CURRENCYID_CURR)),
        PENLT
        AS (SELECT FK_INTERESTID_INTE,
                   FK_CURRENCYID_CURR,
                   MAX (VALIDITY_DATE) AS MAXVALDATE,
                   MAX (PERCENTAGE) AS PERCENTAGE,
                   COUNT (*) AS CNT
              FROM INT_SCALE PENLT, BANK_PARAMETERS
             WHERE VALIDITY_DATE <= SCHEDULED_DATE
            GROUP BY FK_INTERESTID_INTE, FK_CURRENCYID_CURR),
        SOD
        AS (SELECT LSD.ACCOUNT_NUMBER,
                   MAX ( DECODE ( PRIORITY_ORDER ,1, TRIM (SO_ACCOUNT_NUM) || '-' || SO_ACCOUNT_CD ,' ')) AS SO_FIRST_PRIORITY_ACCOUNT_NO,
                   MAX ( DECODE ( PRIORITY_ORDER ,2, TRIM (SO_ACCOUNT_NUM) || '-' || SO_ACCOUNT_CD ,' ')) AS SO_SECOND_PRIORITY_ACCOUNT_NO
              FROM LNS_STAND_DEPOS LSD
             WHERE LSD.PRFT_SYSTEM = 4
               AND LSD.SO_PRFT_SYSTEM = 3
               AND ENTRY_STATUS = '1'
            GROUP BY LSD.ACCOUNT_NUMBER),
        L1
        AS (SELECT L.ACC_TYPE,
                   L.ACC_SN,
                   L.FK_UNITCODE,
                   L.FK_LOANFK_PRODUCTI,
                   L.FKCUR_IS_MOVED_IN,
                   -1 * (  L.NRM_CAP_BAL + L.NRM_RL_INT_BAL + L.NRM_EXP_BAL + L.NRM_COM_BAL + L.OV_CAP_BAL + L.OV_RL_NRM_INT_BAL + L.OV_RL_PNL_INT_BAL + L.OV_EXP_BAL + L.OV_COM_BAL) AS BOOK_BALANCE,
                   -1 * (  L.NRM_CAP_BAL + L.NRM_RL_INT_BAL + L.NRM_EXP_BAL + L.NRM_COM_BAL) AS NRM_BALANCE,
                   -1 * (  OV_CAP_BAL + OV_RL_NRM_INT_BAL + OV_RL_PNL_INT_BAL + OV_EXP_BAL + OV_COM_BAL) AS OV_BALANCE,
                   OV_URL_NRM_INT_BAL + OV_URL_PNL_INT_BAL AS OV_MKT,
                   L.FKGD_HAS_AS_CLASS,
                   L.FKGD_CATEGORY,
                   L.DEP_ACC_SN,
                   L.FKGH_CATEGORY,
                   L.LOAN_STATUS,
                   L.FKGH_HAS_AS_CLASS,
                   (CASE TO_CHAR ( ADD_MONTHS ( (SELECT SCHEDULED_DATE FROM BANK_PARAMETERS) ,-1) ,'yyyyMON')
                      WHEN LST_APR_YEAR || 'APR'  THEN   LST_APR_INT_DB_AMN
                      WHEN LST_AUG_YEAR || 'AUG'  THEN   LST_AUG_INT_DB_AMN
                      WHEN LST_DEC_YEAR || 'DEC'  THEN   LST_DEC_INT_DB_AMN
                      WHEN LST_FEB_YEAR || 'FEB'  THEN   LST_FEB_INT_DB_AMN
                      WHEN LST_JAN_YEAR || 'JAN'  THEN   LST_JAN_INT_DB_AMN
                      WHEN LST_JUL_YEAR || 'JUL'  THEN   LST_JUL_INT_DB_AMN
                      WHEN LST_JUN_YEAR || 'JUN'  THEN   LST_JUN_INT_DB_AMN
                      WHEN LST_MAR_YEAR || 'MAR'  THEN   LST_MAR_INT_DB_AMN
                      WHEN LST_MAY_YEAR || 'MAY'  THEN   LST_MAY_INT_DB_AMN
                      WHEN LST_NOV_YEAR || 'NOV'  THEN   LST_NOV_INT_DB_AMN
                      WHEN LST_OCT_YEAR || 'OCT'  THEN   LST_OCT_INT_DB_AMN
                      WHEN LST_SEP_YEAR || 'SEP'  THEN   LST_SEP_INT_DB_AMN
                   END) AS LAST_MONTH_INTEREST_DEBIT_AMT,
                   L.TOT_INT_SPRD_AMN,
                   L.TOT_SUBS_INT_AMN,
                   L.OV_INST_CNT,
                   L.NRM_CAP_BAL,
                   L.NRM_COM_BAL,
                   L.NRM_EXP_BAL,
                   L.NRM_ACR_INT_BAL,
                   L.NRM_RL_INT_BAL,
                   L.NRM_URL_INT_BAL,
                   L.OV_CAP_BAL,
                   L.OV_COM_BAL,
                   L.OV_EXP_BAL,
                   L.OV_RL_NRM_INT_BAL,
                   L.OV_RL_PNL_INT_BAL,
                   L.OV_URL_NRM_INT_BAL,
                   L.OV_URL_PNL_INT_BAL,
                   L.DRAWDOWN_EXP_DT,
                   L.LST_TRX_DT,
                   L.ACC_MECHANISM,
                   L.OV_EXP_DT,
                   L.DRAWDOWN_FST_AMN,
                   L.DRAWDOWN_FST_DT,
                   L.FKGH_CBPURP,
                   L.FKGD_CBPURP,
                   L.FKINT_CURRENT_FIX,
                   L.FKINT_AS_FLOATING,
                   L.FKINT_PREV_FIXED,
                   L.ACC_OPEN_DT,
                   L.ACC_EXP_DT,
                   L.ACC_STATUS,
                   L.LAST_NRM_TRX_CNT,
                   L.FKCUR_IS_CHARGED,
                   L.FKCUR_USES_AS_LIM,
                   L.FKGH_HAS_AS_FINANC,
                   L.FKGD_HAS_AS_FINANC,
                   L.FKGD_HAS_AS_LOAN_P,
                   ( NRM_URL_INT_BAL + OV_URL_NRM_INT_BAL + OV_URL_PNL_INT_BAL) AS LNS_UNREALIZED,
                   L.ACC_LIMIT_AMN,
                   L.REQ_INSTALL_SN,
                   L.INSTALL_FIRST_DT,
                   L.INSTALL_NEXT_DT,
                   L.INSTALL_PREV_DT,
                   L.TOT_DRAWDOWN_AMN,
                   L.TOT_COMMISSION_AMN,
                   L.TOT_PNL_INT_AMN,
                   L.TOT_EXPENSE_AMN,
                   L.TOT_NRM_INT_AMN,
                   L.TOT_CAP_AMN,
                   L.NRM_SUBSIDY_BAL,
                   L.OV_ACR_NRM_INT_BAL,
                   L.OV_ACR_PNL_INT_BAL,
                   L.OV_SUBSIDY_BAL,
                   L.INSTALL_FREQ,
                   L.INSTALL_COUNT,
                   L.INSTALL_SKIP_CAP,
                   L.INSTALL_SKIP_INT,
                   (CASE WHEN L.PRV_FX_INT_EXP_DT >= BP.CURR_TRX_DATE  THEN L.FKINT_PREV_FIXED
                         WHEN L.CUR_FX_INT_EXP_DT >= BP.CURR_TRX_DATE  THEN L.FKINT_CURRENT_FIX
                         ELSE L.FKINT_AS_FLOATING
                   END) AS IRATE,
                   L.FKGH_HAS_AS_LOAN_P
              FROM BANK_PARAMETERS BP INNER JOIN R_LOAN_ACCOUNT L ON (1 = 1)),
        LFEES
        AS (SELECT FK_LOANFK_PRODUCTI AS PRODUCT_CODE,
                   COM_SCALE.FK_CURRENCYID_CURR AS CURRENCY_ID,
                   MAX (TO_AMOUNT) AS UPPER_AMT,
                   MAX (FROM_AMOUNT) AS LOWER_AMT,
                   SUM (COMMISSION) * 1.10 AS LEDGER_FEES
              FROM BANK_PARAMETERS BP,
                   LOAN_TRXJUST
                   LEFT JOIN COM_SCALE
                     ON (LOAN_TRXJUST.FK_PRFT_TRANSACID = 74131
                         AND LOAN_TRXJUST.FK_JUSTIFICID_JUST = 74131
                         AND LOAN_TRXJUST.FK_LNS_COMMISSION = FK_COMMISSIONID_CO
                         AND ENTRY_STATUS = '1')
             WHERE BP.SCHEDULED_DATE >= COM_SCALE.VALIDITY_DATE
            GROUP BY FK_LOANFK_PRODUCTI, COM_SCALE.FK_CURRENCYID_CURR),
        LAT
        AS (SELECT LAT.ACC_UNIT,
                   LAT.ACC_TYPE,
                   LAT.ACC_SN,
                   SUM (LAT.CAPITAL_DB) AS TOTAL_CAPITAL_DEBITED,
                   SUM (LAT.ACR_NRM_INT_CR + LAT.ACR_PNL_INT_CR + LAT.ACR_PUB_INT_CR + LAT.RL_INT_CR + LAT.URL_INT_CR + LAT.URL_PNL_INT_CR + LAT.URL_PUB_INT_CR) AS TOTAL_INTEREST_CREDITED
              FROM LOAN_ACCOUNT_TOTAL LAT
            GROUP BY LAT.ACC_UNIT, LAT.ACC_TYPE, LAT.ACC_SN)
   SELECT PROFITS_ACCOUNT0.LNS_OPEN_UNIT,
          C.UNIT_NAME,
          PROFITS_ACCOUNT0.ACCOUNT_NUMBER,
          PROFITS_ACCOUNT0.ACCOUNT_CD,
          PROFITS_ACCOUNT0.PRFT_SYSTEM,
          PROFITS_ACCOUNT_1.ACCOUNT_NUMBER AS AGREEMENT_NUMBER,
          PROFITS_ACCOUNT_1.ACCOUNT_CD AS AGREEMENT_CD,
          PROFITS_ACCOUNT_1.PRFT_SYSTEM AS AGREEMENT_SYSTEM,
          R_AGREEMENT.AGR_LIMIT,
          CUST.CUST_ID,
          CUST.C_DIGIT,
          CUST.SURNAME,
          CUST.FIRST_NAME,
          B.ID_PRODUCT,
          B.DESCRIPTION AS PRODUCT_DESC,
          L1.TOT_DRAWDOWN_AMN,
          L1.TOT_COMMISSION_AMN,
          L1.TOT_PNL_INT_AMN,
          L1.TOT_EXPENSE_AMN,
          L1.TOT_NRM_INT_AMN,
          L1.TOT_CAP_AMN,
          A.PROVISION_CURR_PERC AS PROVISION_CURR_PERC,
          L1.NRM_CAP_BAL,
          L1.NRM_RL_INT_BAL,
          L1.NRM_URL_INT_BAL,
          L1.NRM_EXP_BAL,
          L1.NRM_COM_BAL,
          L1.NRM_ACR_INT_BAL,
          L1.NRM_SUBSIDY_BAL,
          L1.OV_CAP_BAL,
          L1.OV_RL_NRM_INT_BAL,
          L1.OV_RL_PNL_INT_BAL,
          L1.OV_URL_NRM_INT_BAL,
          L1.OV_URL_PNL_INT_BAL,
          L1.OV_EXP_BAL,
          L1.OV_COM_BAL,
          L1.OV_ACR_NRM_INT_BAL,
          L1.OV_ACR_PNL_INT_BAL,
          L1.OV_SUBSIDY_BAL,
          DECODE (L1.ACC_TYPE, 14, 0, L1.INSTALL_FREQ) AS INSTALL_FREQ,
          L1.INSTALL_COUNT,
          L1.REQ_INSTALL_SN AS INSTA_PAID,
          L1.INSTALL_FIRST_DT,
          L1.INSTALL_NEXT_DT,
          L1.INSTALL_PREV_DT,
          L1.ACC_EXP_DT,
          L1.ACC_LIMIT_AMN,
          L1.ACC_OPEN_DT,
          L1.BOOK_BALANCE,
          L1.BOOK_BALANCE * NVL (FR.RATE, 1) AS EURO_BOOK_BAL,
          L1.NRM_BALANCE,
          L1.OV_BALANCE,
          A.NRM_RL_URL_FLG,
          A.OV_RL_URL_FLG,
          NVL2 (SDP.DEP_ACC_NUMBER ,SDP.DEBIT_INTEREST_RATE ,NVL (SC.PERCENTAGE, 0) + N128.PERCENTAGE + SPRD.SPREAD) AS FINAL_INTEREST,
          SC.FK_INTERESTID_INTE,
          I.DESCRIPTION AS LNS_INTEREST_DESC,
          NVL (SC.BANK_SPREAD, 0) AS BANK_SPREAD,
          NVL (SC.FK_BASE_RATEFK_GD, 0) AS FK_BASE_RATEFK_GD,
          GD.DESCRIPTION,
          NVL (SC.PERCENTAGE, 0) - NVL (SC.BANK_SPREAD, 0) AS BASE_SPREAD,
          SPRD.SPREAD AS SPREAD,
          N128.PERCENTAGE AS N128,
          L1.OV_MKT,
          A.POSITIVE_AMN,
          A.UNCLEAR_AMN,
          A.BLOCKED_AMN,
          A.DORMANT_AMN,
          A.SELECTED_BANK_RATE,
          A.SELECTED_NORMAL_RA,
          -1 * A.INSTALL_FIXED_AMN AS INSTALL_FIXED_AMN,
          A.NRM_ACCRUAL_AMN,
          A.OV_ACCRUAL_AMN,
          LNS_UNREALIZED,
          0 AS PRFT_ACC_LMT_AMN,
          CLASS_GL1.DR_CNTR_GL_ACC AS NRM_DR_CNTR_GL_ACC,
          CLASS_GL1.CR_CNTR_GL_ACC AS NRM_CR_CNTR_GL_ACC,
          CLASS_GL_1.DR_CNTR_GL_ACC AS OV_DR_CNTR_GL_ACC,
          CLASS_GL_2.DR_CNTR_GL_ACC AS DEFINATE_DELAY,
          CLASS_GL_3.DR_CNTR_GL_ACC AS WRITE_OFF,
		      NVL(LI.NUM_DATA,0) AS MEDIATOR_CODE,
		      LI.TEXT_DATA AS MEDIATOR_DESCRIPTION,
          L1.LOAN_STATUS,
          L1.ACC_STATUS,
          L1.ACC_SN,
          L1.ACC_TYPE,
          L1.FK_UNITCODE,
          L1.LAST_NRM_TRX_CNT,
          L1.FKCUR_IS_CHARGED,
          L1.FKCUR_IS_MOVED_IN,
          L1.FKCUR_USES_AS_LIM,
          L1.FKGD_HAS_AS_CLASS,
          L1.FKGH_HAS_AS_FINANC,
          L1.FKGD_HAS_AS_FINANC,
          L1.FKGD_HAS_AS_LOAN_P,
          L1.FKGH_CATEGORY,
          L1.FKGD_CATEGORY,
          (SELECT SCHEDULED_DATE FROM BANK_PARAMETERS) AS EOM_DATE,
          (CASE WHEN COALESCE (L1.ACC_EXP_DT, SDP.EXPIRY_DATE) <= (SELECT SCHEDULED_DATE FROM BANK_PARAMETERS) THEN 0
                ELSE COALESCE (L1.ACC_EXP_DT, SDP.EXPIRY_DATE) - (SELECT SCHEDULED_DATE FROM BANK_PARAMETERS)
          END) AS REMAINING_DAYS,
          (CASE WHEN COALESCE (L1.ACC_EXP_DT, SDP.EXPIRY_DATE) <= (SELECT SCHEDULED_DATE FROM BANK_PARAMETERS) THEN 0
                ELSE ROUND ( MONTHS_BETWEEN ( COALESCE (L1.ACC_EXP_DT, SDP.EXPIRY_DATE) , (SELECT SCHEDULED_DATE FROM BANK_PARAMETERS)) ,0)
          END) AS REMAINING_MONTHS,
          (CASE WHEN ACC_EXP_DT <= ACC_OPEN_DT THEN 0
                ELSE DAYS(ACC_EXP_DT) - DAYS(ACC_OPEN_DT)
          END) AS TOTAL_DAYS,
          (CASE WHEN ACC_EXP_DT <= ACC_OPEN_DT THEN 0
                ELSE ROUND (MONTHS_BETWEEN (ACC_EXP_DT, ACC_OPEN_DT), 0)
          END) AS TOTAL_MONTHS,
          (CASE WHEN L1.OV_BALANCE = 0 THEN 0
                WHEN L1.OV_EXP_DT < DATE '1900-01-01' THEN 0
                WHEN L1.LOAN_STATUS IN ('2', '3', '4') THEN DAYS((SELECT SCHEDULED_DATE FROM BANK_PARAMETERS)) - DAYS(L1.OV_EXP_DT)
                ELSE 0
          END) AS OVERDUE_DAYS,
          (CASE WHEN OV_BALANCE = 0  THEN 0
                WHEN L1.OV_EXP_DT < DATE '1900-01-01'  THEN  0
                WHEN L1.LOAN_STATUS IN ('2', '3', '4') THEN  ROUND (MONTHS_BETWEEN ( (SELECT SCHEDULED_DATE FROM BANK_PARAMETERS) ,L1.OV_EXP_DT) ,0)
                ELSE 0
          END) AS OVERDUE_MONTHS,
          L1.ACC_MECHANISM,
          L1.OV_EXP_DT,
          L1.DRAWDOWN_FST_AMN,
          L1.DRAWDOWN_FST_DT,
          0 AS PROVISION_AMN,
          L1.FKGH_CBPURP,
          L1.FKGD_CBPURP,
          FKINT_CURRENT_FIX,
          FKINT_AS_FLOATING,
          FKINT_PREV_FIXED,
          PENLT.PERCENTAGE AS PENALTY_INTE_RATE,
          CUST_TYPE,
          CLASS_GL_1.GL_ACCRUAL_ACC AS OV_GL_ACCRUAL_ACC,
          CLASS_GL_2.GL_ACCRUAL_ACC AS DEF_GL_ACCRUAL_ACC,
          LN.LOAN_TYPE,
          DECODE ( LN.LOAN_TYPE,
                  1, 'Short Term Loan',
                  2, 'Fixed Maturity Open Loan',
                  3, 'Open Loan with Promissory Notes',
                  4, 'Amortization Loan (even installments)',
                  5, 'Amortized Capital(even installments)',
                  6, 'Amortized Capital(uneven installments)',
                  7, 'Open Credit',
                  'n/a')  AS LOAN_TYPE_NAME,
          NVL (A.LOAN_SUB_CLASS, 0) AS CURR_SUB_CLASS,
          A.PROVISION_AMOUNT,
          0 AS INTEREST_IN_SUSPENSE,
          0 AS DISCOUNTED_VALUE,
          LN.FKGH_AS_CRED_LINE,
          LN.FKGD_AS_CRED_LINE,
          L1.DRAWDOWN_EXP_DT,
          ' ' AS HOLDR_DESCRIPTION,
          L1.LST_TRX_DT,
          PROFITS_ACCOUNT0.MONOTORING_UNIT,
          A.LOAN_CLASS,
          ( NRM_CAP_BAL + NRM_COM_BAL + NRM_EXP_BAL  + NRM_ACR_INT_BAL   + NRM_RL_INT_BAL    + NRM_URL_INT_BAL +
            OV_CAP_BAL  + OV_COM_BAL  + OV_EXP_BAL   + OV_RL_NRM_INT_BAL + OV_RL_PNL_INT_BAL + OV_URL_NRM_INT_BAL + OV_URL_PNL_INT_BAL +
            A.NRM_ACCRUAL_AMN + A.OV_ACCRUAL_AMN + A.DORMANT_AMN + A.UNCLEAR_AMN) * -1
          + NVL (SDP.ACCR_EXCESS_INTER, 0)
          - NVL (SDP.ACCR_EXC_PROGRESS, 0)   AS GROSS_TOTAL,
          A.ACC_DRAWDOWN_STS,
          NVL (FR.RATE, 1) AS "FIXING_RATE",
          CUR.SHORT_DESCR AS CURRENCY,
          PROFITS_ACCOUNT0.ACCOUNT_SER_NUM,
          PROFITS_ACCOUNT_1.ACCOUNT_SER_NUM AS AGREEM_ACCT_KEY,
          SERVICEACC.ACCOUNT_SER_NUM AS SERVICE_DEP_ACCT_KEY,
          (CASE L1.ACC_STATUS WHEN '3' THEN 'Yes' ELSE 'No' END) AS CLOSED_FLAG,
          L1.TOT_INT_SPRD_AMN,
          L1.TOT_SUBS_INT_AMN,
          L1.OV_INST_CNT,
          CUST.NAME_STANDARD AS CUSTOMER_NAME,
          L1.LAST_MONTH_INTEREST_DEBIT_AMT,
          TRIM (PROFITS_ACCOUNT0.ACCOUNT_NUMBER) || '-' || PROFITS_ACCOUNT0.ACCOUNT_CD AS ACCOUNT_NO,
          W_STG_CRM_ACCT_NOTICE.ARREARS_DATE AS CRM_ARREARS_DATE,
          W_STG_CRM_ACCT_NOTICE.LR_NUMBER AS CRM_LAND_REGISTRY_NUMBER,
          W_STG_CRM_ACCT_NOTICE.NOTICE_ISSUE_DATE_90 AS CRM_NOTICE_ISSUE_DATE_90,
          W_STG_CRM_ACCT_NOTICE.NOTICE_MATURITY_DATE_90 AS CRM_NOTICE_MATURITY_DATE_90,
          W_STG_CRM_ACCT_NOTICE.NOTICE_ISSUE_DATE_40 AS CRM_NOTICE_ISSUE_DATE_40,
          W_STG_CRM_ACCT_NOTICE.NOTICE_MATURITY_DATE_40 AS  CRM_NOTICE_MATURITY_DATE_40,
          W_STG_CRM_ACCT_NOTICE.AUCTION_DATE AS CRM_AUCTION_DATE,
          W_STG_CRM_ACCT_NOTICE.COMMENTS AS CRM_COMMENTS,
          A.RECOVERIES_DT AS RECOVERIES_DATE,
          0 AS MONTHLY_PREMIUM,
          NVL (LFEES.LEDGER_FEES, 0) * FR.REVERSE_RATE AS LEDGER_FEES,
          DELAYOFFICER.ID AS DELAY_OFFICER_ID,
          TRIM (DELAYOFFICER.FIRST_NAME) || ' ' || DELAYOFFICER.LAST_NAME AS DELAY_OFFICER_NAME,
          FKGH_HAS_AS_CLASS,
          DECODE (L1.LOAN_STATUS,
                  '1', 'Normal',
                  '2', 'Overdue',
                  '3', 'Definate Delay',
                  '4', 'Write Off',
                  'n/a')  AS LOAN_STATUS_IND_NAME,
          CLOAN.DESCRIPTION AS CLOAN_CATEGORY_DESCRIPTION,
          DECODE (PROFITS_ACCOUNT0.ACC_STATUS, '3', 0, 1)
          * (CASE WHEN L1.ACC_STATUS = '3' THEN   0
                  WHEN L1.ACC_MECHANISM NOT IN (4, 5)  THEN  0
                  ELSE
                      (CASE WHEN LBDIN.SERIAL_NUM > 0 AND L1.INSTALL_COUNT <> 0  THEN
                                 (L1.INSTALL_SKIP_CAP + L1.INSTALL_SKIP_INT) / L1.INSTALL_COUNT
                            ELSE  A.INSTALL_FIXED_AMN * -1 + NVL (Y.LEDGER_FEE, 0) + NVL (T.INSURANCE_AMOUNT, 0)
                      END)
            END) AS INSTALLMENT_AMOUNT,
          LOANOFFICER.ID LOAN_OFFICER_ID,
          TRIM (LOANOFFICER.FIRST_NAME) || ' ' || LOANOFFICER.LAST_NAME AS LOAN_OFFICER_NAME,
          W_STG_CRM_ACCT_NOTICE.AMOUNT_PAID AS CRM_AMOUNT_PAID,
          W_STG_CRM_ACCT_NOTICE.SALE_PRICE AS CRM_SALE_PRICE,
          W_STG_CRM_ACCT_NOTICE.SALE_DATE AS CRM_SALE_DATE,
          W_STG_CRM_ACCT_NOTICE.COMPLETION_DATE AS CRM_COMPLETION_DATE,
          W_STG_CRM_ACCT_NOTICE.AUCTIONEER,
          0 AS INSURANCES_AMOUNT,
          NVL2 (SDP.DEP_ACC_NUMBER, 'Overdraft', 'Non-Overdraft') AS OVERDRAFT_TYPE_FLAG,
          ALCHS.DESCRIPTION AS SALESPERSON,
          FINSC.DESCRIPTION AS FINANCIAL_SECTOR,
          SOD.SO_FIRST_PRIORITY_ACCOUNT_NO,
          SOD.SO_SECOND_PRIORITY_ACCOUNT_NO,
          LAT.TOTAL_CAPITAL_DEBITED,
          LAT.TOTAL_INTEREST_CREDITED,
          CM.UNIT_NAME AS MONITORING_UNIT_NAME,
          L1.FKGH_HAS_AS_LOAN_P
     FROM L1
          INNER JOIN BANK_PARAMETERS BP ON (1 = 1)
          INNER JOIN PROFITS_ACCOUNT PROFITS_ACCOUNT0
             ON (PROFITS_ACCOUNT0.LNS_OPEN_UNIT = L1.FK_UNITCODE
                 AND PROFITS_ACCOUNT0.LNS_SN = L1.ACC_SN
                 AND PROFITS_ACCOUNT0.LNS_TYPE = L1.ACC_TYPE
                 AND PROFITS_ACCOUNT0.PRFT_SYSTEM = 4
                 AND PROFITS_ACCOUNT0.SECONDARY_ACC <> '1')
          INNER JOIN LOAN LN
             ON (L1.FK_LOANFK_PRODUCTI = LN.FK_PRODUCTID_PRODU)
          INNER JOIN R_LOAN_ACCOUNT_INF A
             ON (L1.FK_UNITCODE = A.FK_LOAN_ACCOUNTFK
                 AND L1.ACC_TYPE = A.FK0LOAN_ACCOUNTACC
                 AND L1.ACC_SN = A.FK_LOAN_ACCOUNTACC)
          LEFT JOIN N128
            ON (LN.FKINT_HAS_N128_INT = N128.FK_INTERESTID_INTE
                AND N128.FK_CURRENCYID_CURR = L1.FKCUR_IS_MOVED_IN)
          --LEFT JOIN SCALE0
          --  ON (N128.VALIDITY_DATE = SCALE0.MAXVALDATE
          --      AND N128.FK_INTERESTID_INTE = SCALE0.FK_INTERESTID_INTE)
          LEFT JOIN LNS_INTEREST I ON (I.ID_INTEREST = L1.IRATE)
          LEFT JOIN W_STG_LOAN_RATEVALDATE LRV
            ON (LRV.ACCOUNT_NUMBER = PROFITS_ACCOUNT0.ACCOUNT_NUMBER)
          LEFT JOIN INT_SCALE SC
            ON (SC.FK_INTERESTID_INTE = I.ID_INTEREST
                AND SC.VALIDITY_DATE = LRV.RATE_VAL_DATE
                AND SC.FK_CURRENCYID_CURR = L1.FKCUR_IS_MOVED_IN
                AND L1.BOOK_BALANCE BETWEEN SC.FROM_AMOUNT AND SC.TO_AMOUNT)
          LEFT JOIN PRODUCT B ON (PROFITS_ACCOUNT0.PRODUCT_ID = B.ID_PRODUCT)
          LEFT JOIN W_STG_CUSTOMER CUST
            ON (PROFITS_ACCOUNT0.CUST_ID = CUST.CUST_ID)
          LEFT JOIN UNIT C ON (C.CODE = PROFITS_ACCOUNT0.LNS_OPEN_UNIT)
          LEFT JOIN UNIT CM ON (CM.CODE = PROFITS_ACCOUNT0.MONOTORING_UNIT)
          LEFT JOIN R_AGREEMENT
            ON (PROFITS_ACCOUNT0.AGR_UNIT = R_AGREEMENT.FK_UNITCODE
                AND PROFITS_ACCOUNT0.AGR_MEMBERSHIP_SN = R_AGREEMENT.AGR_MEMBERSHIP_SN
                AND PROFITS_ACCOUNT0.AGR_SN = R_AGREEMENT.AGR_SN
                AND PROFITS_ACCOUNT0.AGR_YEAR = R_AGREEMENT.AGR_YEAR)
          LEFT JOIN GENERIC_DETAIL GD
            ON (GD.SERIAL_NUM = SC.FK_BASE_RATEFK_GD
                AND FK_GENERIC_HEADPAR = 'BRATE')
          LEFT JOIN PROFITS_ACCOUNT PROFITS_ACCOUNT_1
            ON (PROFITS_ACCOUNT_1.AGR_UNIT = R_AGREEMENT.FK_UNITCODE
                AND PROFITS_ACCOUNT_1.AGR_MEMBERSHIP_SN = R_AGREEMENT.AGR_MEMBERSHIP_SN
                AND PROFITS_ACCOUNT_1.AGR_SN = R_AGREEMENT.AGR_SN
                AND PROFITS_ACCOUNT_1.AGR_YEAR = R_AGREEMENT.AGR_YEAR
                AND PROFITS_ACCOUNT_1.PRFT_SYSTEM = 19)
          LEFT JOIN CLASS_GL CLASS_GL1
            ON (L1.FK_LOANFK_PRODUCTI = CLASS_GL1.FK_PRODUCTID_PRODU
                AND L1.FKGD_CATEGORY = CLASS_GL1.FK_CUST_CATEG_GD
                AND L1.FKGD_HAS_AS_CLASS = CLASS_GL1.FK_GENERIC_DETASER
                AND CLASS_GL1.LOAN_STATUS = '1')
          LEFT JOIN CLASS_GL CLASS_GL_1
            ON (L1.FK_LOANFK_PRODUCTI = CLASS_GL_1.FK_PRODUCTID_PRODU
                AND L1.FKGD_HAS_AS_CLASS = CLASS_GL_1.FK_GENERIC_DETASER
                AND L1.FKGD_CATEGORY = CLASS_GL_1.FK_CUST_CATEG_GD
                AND CLASS_GL_1.LOAN_STATUS = '2')
          LEFT JOIN CLASS_GL CLASS_GL_2
            ON (L1.FKGD_HAS_AS_CLASS = CLASS_GL_2.FK_GENERIC_DETASER
                AND L1.FK_LOANFK_PRODUCTI = CLASS_GL_2.FK_PRODUCTID_PRODU
                AND L1.FKGD_CATEGORY = CLASS_GL_2.FK_CUST_CATEG_GD
                AND CLASS_GL_2.LOAN_STATUS = '3')
          LEFT JOIN CLASS_GL CLASS_GL_3
            ON (L1.FKGD_HAS_AS_CLASS = CLASS_GL_3.FK_GENERIC_DETASER
                AND L1.FK_LOANFK_PRODUCTI = CLASS_GL_3.FK_PRODUCTID_PRODU
                AND L1.FKGD_CATEGORY = CLASS_GL_3.FK_CUST_CATEG_GD
                AND CLASS_GL_3.LOAN_STATUS = '4')
          LEFT JOIN PENLT
            ON (PENLT.FK_INTERESTID_INTE = LN.FKINT_HAS_OVERDUE
                AND PENLT.FK_CURRENCYID_CURR = L1.FKCUR_IS_MOVED_IN)
          LEFT JOIN CURRENCY CUR ON (CUR.ID_CURRENCY = L1.FKCUR_IS_MOVED_IN)
          LEFT JOIN CURRENCY NATIONALCUR ON (NATIONALCUR.NATIONAL_FLAG = '1')
          LEFT JOIN PROFITS_ACCOUNT SERVICEACC
            ON (SERVICEACC.DEP_ACC_NUMBER = L1.DEP_ACC_SN
                AND L1.DEP_ACC_SN <> 0
                AND SERVICEACC.SECONDARY_ACC <> '1'
                AND SERVICEACC.PRFT_SYSTEM = 3)
          LEFT JOIN W_STG_CRM_ACCT_NOTICE
            ON (W_STG_CRM_ACCT_NOTICE.ACCOUNT_NUMBER = PROFITS_ACCOUNT0.ACCOUNT_NUMBER)
                --AND PROFITS_ACCOUNT0.PRFT_SYSTEM = 4)
          LEFT JOIN LFEES
            ON (PROFITS_ACCOUNT0.PRODUCT_ID = LFEES.PRODUCT_CODE
                AND NATIONALCUR.ID_CURRENCY = LFEES.CURRENCY_ID)
          LEFT JOIN BANKEMPLOYEE LOANOFFICER
            ON (LOANOFFICER.ID = R_AGREEMENT.FK_BANKEMPLOYEEID)
          LEFT JOIN BANKEMPLOYEE DELAYOFFICER
            ON (DELAYOFFICER.ID = R_AGREEMENT.FK0BANKEMPLOYEEID)
          LEFT JOIN GENERIC_DETAIL CLOAN
            ON (CLOAN.FK_GENERIC_HEADPAR = L1.FKGH_CATEGORY
                AND CLOAN.SERIAL_NUM = L1.FKGD_CATEGORY)
          LEFT JOIN GENERIC_DETAIL LBDIN
            ON (LBDIN.FK_GENERIC_HEADPAR = 'LBDIN'
                AND LBDIN.SERIAL_NUM = PROFITS_ACCOUNT0.PRODUCT_ID)
          LEFT JOIN W_EOM_FIXING_RATE FR
            ON (FR.EOM_DATE = BP.SCHEDULED_DATE
                AND FR.CURRENCY_ID = L1.FKCUR_IS_MOVED_IN)
          LEFT JOIN W_STG_LOAN_INSURANCE T
            ON (T.ACCOUNT_NUMBER = PROFITS_ACCOUNT0.ACCOUNT_NUMBER)
          LEFT JOIN W_STG_LOAN_LEDGER_FEE Y
            ON (Y.ACCOUNT_NUMBER = PROFITS_ACCOUNT0.ACCOUNT_NUMBER)
          LEFT JOIN W_STG_LOANSPREAD SPRD
            ON (SPRD.ACCOUNT_NUMBER = PROFITS_ACCOUNT0.ACCOUNT_NUMBER)
          LEFT JOIN W_STG_DEPOSIT_ACCOUNT SDP
            ON (PROFITS_ACCOUNT0.ACCOUNT_NUMBER = SDP.LOAN_ACCOUNT_NUMBER)
          LEFT JOIN GENERIC_DETAIL ALCHS
            ON (ALCHS.FK_GENERIC_HEADPAR = A.FKGH_HAS_SELLER
                AND ALCHS.SERIAL_NUM = A.FKGD_HAS_SELLER)
          LEFT JOIN GENERIC_DETAIL FINSC
            ON (ALCHS.FK_GENERIC_HEADPAR = L1.FKGH_HAS_AS_FINANC
                AND ALCHS.SERIAL_NUM = L1.FKGD_HAS_AS_FINANC)
          LEFT JOIN SOD
            ON (SOD.ACCOUNT_NUMBER = PROFITS_ACCOUNT0.ACCOUNT_NUMBER)
          LEFT JOIN LAT
            ON (LAT.ACC_UNIT = L1.FK_UNITCODE
                AND LAT.ACC_TYPE = L1.ACC_TYPE
                AND LAT.ACC_SN = L1.ACC_SN)
	 		    LEFT JOIN LOAN_ADD_INFO LI
            ON (LI.ACC_SN = L1.ACC_SN
                AND LI.ACC_UNIT = L1.FK_UNITCODE
                AND LI.ACC_TYPE = L1.ACC_TYPE
                AND LI.ROW_ID = 5
                AND LI.ROW_INTERNAL_SN = 1)
	   WHERE ((L1.ACC_TYPE <> 14)
          OR (L1.ACC_TYPE = 14
              AND L1.BOOK_BALANCE > 0
              AND SDP.DEP_ACC_NUMBER IS NOT NULL
              AND SDP.ENTRY_STATUS NOT IN (0, 3, 4)));
UPDATE W_EOM_LOAN_ACCOUNT
   SET ( PROVISION_AMN,
         INTEREST_IN_SUSPENSE,
         DISCOUNTED_VALUE,
         FINAL_SUB_CLASS,
         ADJUSTED_SUB_CLASS,
         ADJUSTED_CLASS,
         ACTUAL_SUB_CLASS,
         COLLATERAL_OM_VALUE,
         DATE_CLASS_CHANGED,
         FINAL_CLASS_NAME) =
          (SELECT PROVISION_AMT,
                  INTEREST_IN_SUSPENSE_AMT,
                  DISCOUNTED_VALUE_AMT,
                  FINAL_SUB_CLASS_IND,
                  ADJUSTED_SUB_CLASS_IND,
                  ADJUSTED_CLASS_IND,
                  ACTUAL_SUB_CLASS_IND,
                  COLLATERAL_OM_VALUE_AMT,
                  DATE_CLASS_CHANGED,
                  FINAL_CLASS_NAME
             FROM W_STG_AGG_ACCT_USERFIELDS
            WHERE W_EOM_LOAN_ACCOUNT.ACCOUNT_NUMBER = ACCOUNT_NUMBER
              AND W_EOM_LOAN_ACCOUNT.PRFT_SYSTEM = PRFT_SYSTEM)
  WHERE EOM_DATE = (SELECT SCHEDULED_DATE FROM BANK_PARAMETERS);
 UPDATE W_EOM_LOAN_ACCOUNT
    SET LC_GROSS_TOTAL = GROSS_TOTAL * "FIXING_RATE"
  WHERE EOM_DATE = (SELECT SCHEDULED_DATE FROM BANK_PARAMETERS);
 UPDATE W_EOM_LOAN_ACCOUNT A
    SET (TRX_AMN_WO_WD) = (SELECT SUM (TRX_AMN) AS TRX_AMN
                             FROM LOAN_ACCOUNT_EXTRA B
                            WHERE B.ACC_UNIT = A.FK_UNITCODE
                              AND B.ACC_TYPE = A.ACC_TYPE
                              AND B.ACC_SN = A.ACC_SN
                              AND TRANSACTION_CODE IN (4981,4211,4221,4241)
                              AND REVERSED_FLG <> '1'
                              AND JUSTIFICATION_CODE NOT IN (SELECT SERIAL_NUM
                                                               FROM GENERIC_DETAIL
                                                              WHERE FK_GENERIC_HEADPAR = 'EOM01'))
  WHERE EOM_DATE = (SELECT SCHEDULED_DATE FROM BANK_PARAMETERS);
 UPDATE W_EOM_LOAN_ACCOUNT A
    SET TRX_AMN_CAP_INT = (SELECT SUM (TRX_AMN) AS TRX_AMN
                             FROM LOAN_ACCOUNT_EXTRA B
                            WHERE B.ACC_UNIT = A.FK_UNITCODE
                              AND B.ACC_TYPE = A.ACC_TYPE
                              AND B.ACC_SN = A.ACC_SN
                              AND TRANSACTION_CODE = 4261
                              AND REVERSED_FLG <> '1'
                              AND REQUEST_TYPE = '3'
                              AND JUSTIFICATION_CODE NOT IN (SELECT SERIAL_NUM
                                                               FROM GENERIC_DETAIL
                                                              WHERE FK_GENERIC_HEADPAR = 'EOM01'))
  WHERE EOM_DATE = (SELECT SCHEDULED_DATE FROM BANK_PARAMETERS);
 UPDATE W_EOM_LOAN_ACCOUNT A
    SET WRITE_OFF_PAY_AMT = (SELECT SUM ( CAPITAL_CR + COMMISSION_CR + EXPENSE_CR + RL_INT_CR + RL_PNL_INT_CR +
                                          URL_INT_CR + URL_PNL_INT_CR + URL_PUB_INT_CR) AS WRITE_OFF_PAYMENT_AMT
                               FROM LOAN_ACCOUNT_TOTAL
                              WHERE LOAN_ACCOUNT_TOTAL.ACC_UNIT = A.FK_UNITCODE
                                AND LOAN_ACCOUNT_TOTAL.ACC_TYPE = A.ACC_TYPE
                                AND LOAN_ACCOUNT_TOTAL.ACC_SN = A.ACC_SN
                                AND ACCOUNT_LOAN_STS = '4'
                             HAVING SUM (CAPITAL_CR + COMMISSION_CR + EXPENSE_CR + RL_INT_CR + RL_PNL_INT_CR +
                                         URL_INT_CR + URL_PNL_INT_CR + URL_PUB_INT_CR) > 0)
  WHERE EOM_DATE = (SELECT SCHEDULED_DATE FROM BANK_PARAMETERS);
END;

