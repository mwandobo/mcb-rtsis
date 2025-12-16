create table DEP_TAX_LMT_HST
(
    LMT_SN             INTEGER        not null,
    TRX_DATE           DATE           not null,
    TRX_UNIT           SMALLINT       not null,
    TRX_CODE           INTEGER        not null,
    ID_JUSTIFIC        INTEGER        not null,
    TOTAL_AMNT         DECIMAL(15, 2) not null,
    UTILIZED_AMNT      DECIMAL(15, 2) not null,
    FK_CUSTOMERCUST_ID INTEGER        not null,
    constraint PK_DEP_TAX_LMT_HST
        primary key (FK_CUSTOMERCUST_ID, LMT_SN)
);

