create table CARD_CONTROLIOR
(
    REC_ID                        DECIMAL(11) not null,
    STATUS                        CHAR(1)     not null,
    STATUS_TRANSFER               CHAR(1)     not null,
    STATUS_TRANSACTION            SMALLINT    not null,
    MIN_AMOUNT                    DECIMAL(11, 2),
    COMM_AMOUNT                   DECIMAL(11, 2),
    TRX_DATE                      DATE,
    TMP_STATUS_UPDATE             TIMESTAMP(6),
    PRINT_CARD_ID                 DECIMAL(11),
    CREDIT_CARD                   CHAR(1),
    TMP                           TIMESTAMP(6),
    FK_CURRENCYID_CURRENCY        INTEGER,
    FK0CURRENCYID_CURRENCY        INTEGER,
    FK_DEPOSIT_ACCOACCOUNT_NUMBER DECIMAL(11),
    FK0DEPOSIT_ACCOACCOUNT_NUMBER DECIMAL(11),
    FK_JUSTIFICID_JUSTIFIC        INTEGER,
    FK0JUSTIFICID_JUSTIFIC        INTEGER,
    FK_PRFT_TRANSACID_TRANSACT    INTEGER,
    FK0PRFT_TRANSACID_TRANSACT    INTEGER,
    FK_USRCODE                    CHAR(8),
    FK0USRCODE                    CHAR(8),
    FK_UNITCODE                   INTEGER,
    constraint PK_CRD_CNT
        primary key (STATUS_TRANSACTION, STATUS_TRANSFER, STATUS, REC_ID)
);

create unique index I0001020
    on CARD_CONTROLIOR (FK_JUSTIFICID_JUSTIFIC);

create unique index I0001022
    on CARD_CONTROLIOR (FK0JUSTIFICID_JUSTIFIC);

create unique index I0001024
    on CARD_CONTROLIOR (FK_PRFT_TRANSACID_TRANSACT);

create unique index I0001026
    on CARD_CONTROLIOR (FK0PRFT_TRANSACID_TRANSACT);

