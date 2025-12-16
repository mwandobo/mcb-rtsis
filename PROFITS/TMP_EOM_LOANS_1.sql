create table TMP_EOM_LOANS_1
(
    EOM_DATE                 DATE     not null,
    ACCOUNT_NUMBER           CHAR(40) not null
        constraint TMP_EOM_LOANS_1_PK
            primary key,
    ACCOUNT_CD               SMALLINT,
    FIRST_NAME               CHAR(20),
    SURNAME                  CHAR(70),
    FK_UNITCODE              INTEGER,
    ACC_TYPE                 SMALLINT,
    ACC_SN                   INTEGER,
    EURO_BOOK_BAL            DECIMAL(15, 2),
    ACCOUNT_TYPE             CHAR(2),
    CUST_TYPE                SMALLINT,
    CUST_ID                  INTEGER,
    AFM_NO                   CHAR(20),
    DRAWDOWN_FST_DT          DATE,
    ACC_LIMIT_AMN            DECIMAL(15, 2),
    NRM_CAP_BAL              DECIMAL(15, 2),
    OV_CAP_BAL               DECIMAL(15, 2),
    NRM_RL_INT_BAL           DECIMAL(15, 2),
    NRM_URL_INT_BAL          DECIMAL(15, 2),
    OV_RL_NRM_INT_BAL        DECIMAL(15, 2),
    OV_RL_PNL_INT_BAL        DECIMAL(15, 2),
    OV_URL_NRM_INT_BAL       DECIMAL(15, 2),
    OV_URL_PNL_INT_BAL       DECIMAL(15, 2),
    NRM_COM_BAL              DECIMAL(15, 2),
    NRM_EXP_BAL              DECIMAL(15, 2),
    OV_COM_BAL               DECIMAL(15, 2),
    OV_EXP_BAL               DECIMAL(15, 2),
    FKGH_HAS_AS_FINANC       CHAR(5),
    FKGD_HAS_AS_FINANC       INTEGER,
    FKGH_AS_CRED_LINE        CHAR(5),
    FKGD_AS_CRED_LINE        INTEGER,
    HOLDR_FK_GENERIC_DETASER INTEGER,
    HOLDR_DESCRIPTION        CHAR(20),
    CURR_SUB_CLASS           CHAR(1),
    AGR_LIMIT                DECIMAL(15, 2),
    GROSS_TOTAL              DECIMAL(15, 2)
);

create unique index IX_ABS
    on TMP_EOM_LOANS_1 (FK_UNITCODE, ACC_TYPE, ACC_SN);

create unique index IX_CUSTID4
    on TMP_EOM_LOANS_1 (CUST_ID);

create unique index PK_LNS1
    on TMP_EOM_LOANS_1 (EOM_DATE, ACCOUNT_NUMBER);

