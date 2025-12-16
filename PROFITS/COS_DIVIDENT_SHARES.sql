create table COS_DIVIDENT_SHARES
(
    SHARE_ID        DECIMAL(10) not null,
    DIVIDENT_DATE   DATE        not null,
    STATUS          SMALLINT,
    CHARGE_TYPE     SMALLINT,
    TRX_UNIT        INTEGER,
    CUSTOMER_ID     INTEGER,
    TRX_SN          INTEGER,
    MEMBER_ID       DECIMAL(10),
    CHARGE_AMOUNT   DECIMAL(15, 2),
    DIVIDENT_RIGHT  DECIMAL(15, 2),
    DIVIDENT_AMOUNT DECIMAL(15, 2),
    TRX_DATE        DATE,
    YEAR_OF_USE     CHAR(4),
    TRX_USR         CHAR(8),
    DIVIDENT_ACC    CHAR(40),
    COMMENTS        CHAR(80),
    SERVICE_PRODUCT INTEGER,
    constraint IXU_CP_109
        primary key (SHARE_ID, DIVIDENT_DATE)
);

