create table LOAN_ACC_EXTR_DET
(
    FK_UNITCODE      INTEGER  not null,
    ACC_TYPE         SMALLINT not null,
    ACC_SN           INTEGER  not null,
    CURR_TRX_DATE    DATE     not null,
    INT_SN           SMALLINT not null,
    TMSTAMP          TIMESTAMP(6),
    EXTRAIT_COMMENTS VARCHAR(40),
    RQ_TYPE_CR_DB    CHAR(2),
    REQUEST_SN       CHAR(3),
    REQUEST_TYPE     CHAR(3),
    TRX_REMAIN       DECIMAL(15, 2),
    TRX_CR           DECIMAL(15, 2),
    TRX_DB           DECIMAL(15, 2),
    TRX_VALEUR       DATE,
    TRX_DESCR        CHAR(40),
    TRANSACTION_CODE INTEGER,
    TRX_DATE         DATE,
    constraint IXU_LOA_104
        primary key (ACC_SN, ACC_TYPE, CURR_TRX_DATE, FK_UNITCODE, INT_SN)
);

