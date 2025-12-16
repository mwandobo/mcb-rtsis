create table COL_MARGIN_INPUT
(
    BUSINESS_DATE    DATE           not null,
    PRFT_ACC_NUM     CHAR(40)       not null,
    PRFT_SYSTEM      SMALLINT       not null,
    ISIN             CHAR(12)       not null,
    PRFT_ACC_NUM_CD  SMALLINT,
    NO_OF_SHARES     DECIMAL(10)    not null,
    CLOSING_PRICE    DECIMAL(18, 4) not null,
    CLOSING_PRICE_DT DATE,
    INSERT_TMSTAMP   TIMESTAMP(6),
    RECORD_STS       CHAR(1),
    ERROR_DESC       CHAR(80),
    UPDATE_TMSTAMP   TIMESTAMP(6),
    constraint PK_COLMARG
        primary key (ISIN, PRFT_SYSTEM, PRFT_ACC_NUM, BUSINESS_DATE)
);

