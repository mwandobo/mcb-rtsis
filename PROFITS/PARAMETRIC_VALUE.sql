create table PARAMETRIC_VALUE
(
    SEC_WINDOW_SN      SMALLINT,
    SNUM               INTEGER,
    PW_NUMBER_18_4     DECIMAL(18, 4),
    PW_TIME            DATE,
    PW_DATE            DATE,
    PW_TMSTAMP         TIMESTAMP(6),
    PW_FLAG_2          CHAR(2),
    SEC_WINDOW_ID      CHAR(8),
    TABLE_ATTRIBUTE    CHAR(40),
    TABLE_ENTITY       CHAR(40),
    ACCOUNT_NUMBER     CHAR(40),
    ALIAS_TABLE_ORIGIN CHAR(40),
    PW_TEXT            VARCHAR(100)
);

create unique index PK_PAR_VL
    on PARAMETRIC_VALUE (ACCOUNT_NUMBER, SEC_WINDOW_ID, SEC_WINDOW_SN, SNUM);

