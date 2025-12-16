create table DATA_ENTRY_DETAIL1
(
    FK0DATA_ENTRY_HTRX DATE        not null,
    FK_DATA_ENTRY_HSER DECIMAL(15) not null,
    FK_DATA_ENTRY_HSUP INTEGER     not null,
    FK_DATA_ENTRY_HTRX INTEGER     not null,
    LINE_NUMBER        INTEGER     not null,
    PSB_LINE_SN        INTEGER,
    TRX_UNIT           INTEGER,
    ACCOUNT_UNIT       INTEGER,
    TRX_USR_SN         INTEGER,
    PSB_SN             INTEGER,
    ACCOUNT_NUMBER     DECIMAL(11),
    TRX_AMOUNT         DECIMAL(15, 2),
    NEW_BALANCE        DECIMAL(15, 2),
    TIMESTMP           DATE,
    TRX_DATE           DATE,
    ACCOUNT_CD         CHAR(2),
    TRX_USR            CHAR(8),
    DESC_STATUS        VARCHAR(35),
    constraint IXU_REP_204
        primary key (FK0DATA_ENTRY_HTRX, FK_DATA_ENTRY_HSER, FK_DATA_ENTRY_HSUP, FK_DATA_ENTRY_HTRX, LINE_NUMBER)
);

