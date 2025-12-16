create table LOAN_CATEGORY
(
    ACC_UNIT        INTEGER      not null,
    ACC_TYPE        SMALLINT     not null,
    ACC_SN          INTEGER      not null,
    ACC_CD          SMALLINT,
    ENTRY_STATUS    CHAR(1),
    INS_TMSTAMP     TIMESTAMP(6) not null,
    DEL_TMSTAMP     TIMESTAMP(6) not null,
    LOAN_GEN_HEADER CHAR(5)      not null,
    LOAN_GEN_DETAIL INTEGER      not null,
    LOAN_GEN_DESC   CHAR(40)     not null,
    INS_USER        CHAR(8),
    DEL_USER        CHAR(8),
    AMNT            DECIMAL(15, 2),
    EXPIRY_DATE     DATE
);

create unique index IXP_LOAN_CAT_001
    on LOAN_CATEGORY (ACC_UNIT, ACC_TYPE, LOAN_GEN_DETAIL, INS_TMSTAMP, LOAN_GEN_HEADER, ACC_SN);

