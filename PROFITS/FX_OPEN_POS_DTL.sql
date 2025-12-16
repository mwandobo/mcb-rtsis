create table FX_OPEN_POS_DTL
(
    DTL_SN                      INTEGER  not null,
    TICKET_DATE                 DATE,
    TICKET_SN                   INTEGER  not null,
    UTILIZED_AMOUNT             DECIMAL(15, 2),
    ID_CURRENCY                 INTEGER  not null,
    TMPSTAMP                    DATE     not null,
    ENTRY_STATUS                CHAR(1)  not null,
    FK_FX_OPEN_POS_ENTRY_STATUS CHAR(1)  not null,
    FK_FX_OPEN_POS_ID_CURRENCY  INTEGER  not null,
    FK_FX_OPEN_POS_TRX_INT_SN   SMALLINT not null,
    FK_FX_OPEN_POS_TRX_SN       INTEGER  not null,
    FK_FX_OPEN_POS_TRX_USR      CHAR(8)  not null,
    FK_FX_OPEN_POS_TRX_UNIT     INTEGER  not null,
    FK_FX_OPEN_POS_TRX_DATE     DATE     not null,
    constraint PK_FX_OPEN_POS_DTL
        primary key (FK_FX_OPEN_POS_TRX_DATE, FK_FX_OPEN_POS_ENTRY_STATUS, FK_FX_OPEN_POS_ID_CURRENCY,
                     FK_FX_OPEN_POS_TRX_INT_SN, FK_FX_OPEN_POS_TRX_SN, FK_FX_OPEN_POS_TRX_USR, FK_FX_OPEN_POS_TRX_UNIT,
                     DTL_SN)
);

