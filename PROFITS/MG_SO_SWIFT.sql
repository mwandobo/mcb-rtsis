create table MG_SO_SWIFT
(
    FILE_NAME            CHAR(50)    not null,
    SERIAL_NO            DECIMAL(10) not null,
    CORRSP_CUST_ID       INTEGER,
    CORRSP_ACCOUNT       CHAR(40),
    OTHER_SYSTEM_ID      CHAR(20),
    BENEF_FULLNAME       VARCHAR(100),
    BENEF_ADDRESS_LINE_1 VARCHAR(100),
    BENEF_ADDRESS_LINE_2 VARCHAR(100),
    BENEF_BIC_ADDRESS    VARCHAR(12),
    BENEF_BANK_ACCOUNT   VARCHAR(40),
    SENDING_METHOD       CHAR(1),
    BANK_CHARGES         CHAR(1),
    SWIFT_STATUS         CHAR(1),
    STP_INDEX            CHAR(1),
    VALUE_DAYS           SMALLINT,
    MIGR_STATUS          CHAR(1),
    MIGR_ERROR_DESC      CHAR(80),
    MIGR_TIMESTAMP       TIMESTAMP(6),
    constraint PK_MG_SO_SWIFT
        primary key (SERIAL_NO, FILE_NAME)
);

