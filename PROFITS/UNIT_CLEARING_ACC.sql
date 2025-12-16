create table UNIT_CLEARING_ACC
(
    FK_UNITCODE    INTEGER not null,
    FK_CURRENCY_ID INTEGER not null,
    ACCOUNT_NUMBER CHAR(40),
    ACCOUNT_CD     SMALLINT,
    TMSTAMP        TIMESTAMP(6),
    ENTRY_STATUS   CHAR(1),
    FK0UNITCODE    INTEGER,
    constraint PK
        primary key (FK_UNITCODE, FK_CURRENCY_ID)
);

