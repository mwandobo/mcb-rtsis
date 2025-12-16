create table PAYMENTS
(
    REC_ID          DECIMAL(10) not null
        constraint IXU_CP_099
            primary key,
    ERROR_CODE      SMALLINT,
    MAIN_REF_NUMBER SMALLINT,
    SERVIS          INTEGER,
    COMMISION       INTEGER,
    CASH            DECIMAL(10, 4),
    PAYD            DECIMAL(10, 5),
    FAMILY_CODE     DECIMAL(11),
    TRAN_CODE       DECIMAL(11),
    PROVIDER_ID     DECIMAL(11),
    COMMISSION_AMNT DECIMAL(15, 2),
    TRX_DATE        DATE,
    TMSTAMP         TIMESTAMP(6),
    MAIN_FLAG       CHAR(1),
    STATUS          CHAR(1),
    PID             CHAR(11),
    PROVIDER_NUM    CHAR(20),
    BRANCH          CHAR(20),
    TEL             CHAR(20),
    USERR           CHAR(20),
    ACC_NUM         CHAR(24),
    TRID            CHAR(25),
    FULL_NAME       CHAR(30)
);

