create table TRMSGOUTHDR
(
    TRX_DATE         DATE     not null,
    SWIFT_NUMBER     SMALLINT not null,
    MSGOUT_SN        SMALLINT not null,
    MSGOUT_FILE_FLAG CHAR(1),
    SWIFT_ADDRESS    VARCHAR(14),
    constraint IXU_DEP_166
        primary key (TRX_DATE, SWIFT_NUMBER, MSGOUT_SN)
);

