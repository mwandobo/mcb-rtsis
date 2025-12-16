create table CUST_TRANSFR_HIST_DTL
(
    FK_HDR_TRX_SN          INTEGER not null,
    FK_HDR_TRX_USR         CHAR(8) not null,
    FK_HDR_TRX_DATE        DATE    not null,
    FK_HDR_TRX_UNIT        INTEGER not null,
    ACTION_SN              INTEGER not null,
    ACCOUNT_CD             SMALLINT,
    PRFT_SYSTEM            SMALLINT,
    CURRENCY_ID            INTEGER,
    JUSTIFIC_ID            INTEGER,
    PRODUCT_ID             INTEGER,
    PREV_UNIT_CODE         INTEGER,
    TRX_USR_SN             INTEGER,
    AMOUNT_1               DECIMAL(15, 2),
    AMOUNT_2               DECIMAL(15, 2),
    PROFITS_ACCOUNT        CHAR(40),
    COLLATERAL_SN          DECIMAL(10),
    COLLATERAL_FK_UNITCODE DECIMAL(5),
    COLLATERAL_TFK         DECIMAL(5),
    constraint IXU_CUS_033
        primary key (FK_HDR_TRX_SN, FK_HDR_TRX_USR, FK_HDR_TRX_DATE, FK_HDR_TRX_UNIT, ACTION_SN)
);

