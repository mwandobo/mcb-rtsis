create table INCOMING_DEP_FILE
(
    PROGRAM_ID   CHAR(5)     not null,
    TRX_DATE     DATE        not null,
    SERIAL_NUM   DECIMAL(10) not null,
    LINE_SN      DECIMAL(10) not null,
    TMSTAMP      TIMESTAMP(6),
    LINE_DATA    VARCHAR(1024),
    ERROR_DESC   CHAR(80),
    ENTRY_STATUS CHAR(1),
    constraint PK_INCOM_DEP_FILE
        primary key (LINE_SN, SERIAL_NUM, TRX_DATE, PROGRAM_ID)
);

