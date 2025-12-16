create table BOP_PARAMETERS
(
    AES_NAME           CHAR(3) not null
        constraint PKBOPPAR
            primary key,
    AET_NAME           CHAR(3),
    LAST_CLOSED_MONTH  CHAR(2),
    AET_SERIES         CHAR(1),
    LAST_CLOSED_YEAR   CHAR(4),
    AES_SERIES         CHAR(1),
    ANNOUNCED_AMOUNT   DECIMAL(15, 2),
    BANK_ID            INTEGER,
    FK_CURR_DEFINED_BY INTEGER
);

