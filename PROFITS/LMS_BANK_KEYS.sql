create table LMS_BANK_KEYS
(
    BANK_CODE            SMALLINT     not null,
    SN                   DECIMAL(10)  not null,
    INSERTION_TMSTAMP    TIMESTAMP(6),
    CURR_TRX_DATE        DATE,
    BANK_ACTIVATION_CODE CHAR(250),
    UPD_INACTIVE_STAMP   TIMESTAMP(6),
    KEY_STATUS           CHAR(1),
    REQ_TIMESTAMP        TIMESTAMP(6) not null,
    REQUEST_SN           INTEGER      not null,
    constraint PK_LMS_10
        primary key (SN, BANK_CODE)
);

