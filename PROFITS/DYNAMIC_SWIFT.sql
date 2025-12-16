create table DYNAMIC_SWIFT
(
    REF_NO        CHAR(16)     not null
        constraint PK_DYN_SWF_RNO
            primary key,
    ORDER_NO      CHAR(16)     not null,
    SWIFT_ID      CHAR(5)      not null,
    STP_FLG       CHAR(1),
    TRX_DATE      DATE         not null,
    TMSTAMP       TIMESTAMP(6) not null,
    TRX_USER      CHAR(8)      not null,
    UPDATE_DATE   DATE         not null,
    SWIFT_STATUS  CHAR(1)      not null,
    SWIFT_DETAILS VARCHAR(2048)
);

