create table W_AGG_GL_UNIT_TOTALS
(
    LST_UPDAT_DATE     DATE           not null,
    CREDIT             DECIMAL(18, 2) not null,
    DEBIT              DECIMAL(18, 2),
    FK_COMPANY_CODE    INTEGER        not null,
    GL_DATE            DATE           not null,
    FK_COST_ID         CHAR(10)       not null,
    FK_CURRENCYID_CURR INTEGER        not null,
    FK_UNITCODE        INTEGER        not null,
    FK_GLG_ACCOUNTACCO CHAR(21)       not null,
    DEBIT_ZB           DECIMAL(18, 2),
    DEBIT_BD           DECIMAL(18, 2),
    CREDIT_ZB          DECIMAL(18, 2),
    CREDIT_BD          DECIMAL(18, 2),
    BALANCE_ZB         DECIMAL(18, 2),
    DAILY_BALANCE_ZB   DECIMAL(18, 2),
    BALANCE            DECIMAL(18, 2),
    DAILY_BALANCE      DECIMAL(18, 2),
    DEBIT_TB           DECIMAL(18, 2) default 0,
    CREDIT_TB          DECIMAL(18, 2) default 0,
    UPDATE_TIMESTAMP   TIMESTAMP(6),
    constraint IXU_GLG_EOM
        primary key (FK_COST_ID, FK_CURRENCYID_CURR, FK_UNITCODE, FK_GLG_ACCOUNTACCO, GL_DATE, FK_COMPANY_CODE)
);

