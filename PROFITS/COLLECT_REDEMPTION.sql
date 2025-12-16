create table COLLECT_REDEMPTION
(
    PRFT_SYSTEM      SMALLINT     not null,
    ACCOUNT_NUMBER   CHAR(40)     not null,
    REDEMPTION_SN    INTEGER      not null,
    TMSTAMP          TIMESTAMP(6) not null,
    ACCOUNT_CD       SMALLINT,
    CUSTOMER_ID      INTEGER,
    INSERT_UNIT      INTEGER      not null,
    INSERT_USR       CHAR(8)      not null,
    INSERT_DATE      DATE         not null,
    UPDATE_UNIT      INTEGER      not null,
    UPDATE_USR       CHAR(8)      not null,
    UPDATE_DATE      DATE         not null,
    REDEMPTION_DT    DATE,
    REDEMPTION_AMN   DECIMAL(15, 2),
    FORECAST_DT      DATE,
    FORECAST_AMN     DECIMAL(15, 2),
    REDEMPTION_STS   CHAR(1),
    REDEMPTION_CMNTS VARCHAR(500),
    constraint PIX_COLRED
        primary key (TMSTAMP, REDEMPTION_SN, ACCOUNT_NUMBER, PRFT_SYSTEM)
);

