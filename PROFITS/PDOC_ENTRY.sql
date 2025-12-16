create table PDOC_ENTRY
(
    SERIAL_NO       INTEGER      not null,
    CUSTOMER        INTEGER      not null,
    ACCOUNT         CHAR(40)     not null,
    PRFT_SYSTEM     INTEGER      not null,
    DOC_CODE        CHAR(2)      not null,
    DOC_CATEGORY    CHAR(100)    not null,
    DOC_TYPE        CHAR(250)    not null,
    TRX_CODE        INTEGER      not null,
    CREATE_TMSTAMP  TIMESTAMP(6) not null,
    PROCESS_TMSTAMP TIMESTAMP(6),
    ENTRY_STATUS    CHAR(1)      not null,
    ENTRY_COMMENTS  VARCHAR(500),
    primary key (SERIAL_NO, CUSTOMER, ACCOUNT, PRFT_SYSTEM, DOC_CODE, DOC_CATEGORY, DOC_TYPE, TRX_CODE, CREATE_TMSTAMP)
);

