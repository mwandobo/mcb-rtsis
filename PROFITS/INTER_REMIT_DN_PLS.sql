create table INTER_REMIT_DN_PLS
(
    FK_INTER_REMIT_REMITT_NUMB_PL DECIMAL(10) not null,
    SN                            SMALLINT    not null,
    REF_KEY_SN                    SMALLINT,
    FK_CURRENCYID_CURRENCY        INTEGER,
    REF_KEY_REMIT                 INTEGER,
    TRX_USR_SN                    INTEGER,
    QUANTITY                      INTEGER,
    DENOMINATION                  DECIMAL(15, 2),
    AMOUNT                        DECIMAL(15, 2),
    ENTRY_STATUS                  CHAR(1),
    ENTRY_COMMENTS                CHAR(40),
    constraint IXU_FX_054
        primary key (FK_INTER_REMIT_REMITT_NUMB_PL, SN)
);

