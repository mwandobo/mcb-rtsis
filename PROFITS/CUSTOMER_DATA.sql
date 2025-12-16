create table CUSTOMER_DATA
(
    SERIAL_NUMBER     INTEGER      not null,
    ORGANIZATION_NAME CHAR(50)     not null,
    ENTRY_STATUS      SMALLINT     not null,
    TRX_DATE          DATE         not null,
    TIMESTAMP         TIMESTAMP(6) not null,
    CUSTOMER_SURNAME  CHAR(30)     not null,
    CUSTOMER_NAME     CHAR(30)     not null,
    CUST_SURNAME_EN   CHAR(30),
    CUST_NAME_EN      CHAR(30),
    BIRTH_DATE        DATE,
    CUST_ADDRESS      CHAR(50),
    HOME_TEL_NUMBER   CHAR(7),
    MOBILE_NUMBER     CHAR(10),
    POSITION          CHAR(20),
    CARD_ID           CHAR(10),
    CARD_PIN          CHAR(12),
    ORGANIZATION_ACC  VARCHAR(40),
    constraint PK_CUSTOMER_DATA
        primary key (SERIAL_NUMBER, ORGANIZATION_NAME, TRX_DATE, TIMESTAMP, ENTRY_STATUS)
);

