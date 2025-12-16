create table DIASPAY_PENSIONER
(
    FILE_DATE         DATE        not null,
    FILE_RECORD_SN    DECIMAL(10) not null,
    RECORD_TYPE       SMALLINT,
    ORGANIZATION_CODE CHAR(5),
    SURNAME           CHAR(20),
    FIRST_NAME        CHAR(6),
    FATHER_NAME       CHAR(4),
    AMP_CODE          CHAR(10),
    IBAN              CHAR(27),
    PROFITS_ACC_NUM   CHAR(40),
    PRFT_SYSTEM       SMALLINT,
    CUST_ID           INTEGER,
    TRX_DATE          DATE,
    GRP_REC_COUNT_IN  INTEGER,
    GRP_REC_COUNT_OUT INTEGER,
    ORGANIZATION_NAME CHAR(30),
    LAST_UPDATE_USER  CHAR(8),
    LAST_UPD_TMSTAMP  DATE,
    SEND_DATE         DATE,
    LAST_UPDATE_UNIT  INTEGER,
    constraint IXU_CP__56
        primary key (FILE_DATE, FILE_RECORD_SN)
);

