create table TRX_SEQ_ACCOUNT
(
    ID_TRANSACT INTEGER  not null,
    SN          SMALLINT not null,
    PRFT_SYSTEM SMALLINT,
    constraint IXU_FX_051
        primary key (ID_TRANSACT, SN)
);

