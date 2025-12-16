create table BILL_DISCREPANCY_DTL
(
    DISCR_HDR_SERIAL_NUM   INTEGER     not null,
    SERIAL_NUM             INTEGER     not null,
    ENTRY_STATUS           VARCHAR(1),
    DINTER_PROGRAM_ID      VARCHAR(5)  not null,
    DINTER_ACH_SETTLE_DATE VARCHAR(8)  not null,
    DINTER_CUTOFF_SN       INTEGER     not null,
    DINTER_IDENTIFIER      INTEGER     not null,
    DFILENAME              VARCHAR(30) not null,
    CODELINE               VARCHAR(255),
    FK_GENERIC_DISCRFK     VARCHAR(5),
    FK_GENERIC_DISCSER     INTEGER,
    constraint PK_DISCRIPANCE_DETAIL
        primary key (SERIAL_NUM, DISCR_HDR_SERIAL_NUM)
);

