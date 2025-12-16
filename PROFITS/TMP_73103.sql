create table TMP_73103
(
    SERIAL_NUMBER  INTEGER not null,
    CUST_ID        INTEGER,
    ACCOUNT_NUMBER CHAR(40),
    PRFT_SYSTEM    INTEGER,
    ENTRY_STATUS   CHAR(1)
);

create unique index PK_73103
    on TMP_73103 (SERIAL_NUMBER);

