create table STAT_ACC_CHEQ_DT
(
    ACCOUNT_NUMBER     DECIMAL(11) not null,
    ITEM_SERIAL_NUMBER DECIMAL(10) not null,
    BOUNCED_DATE       DATE        not null,
    ENTRY_STATUS       CHAR(1),
    constraint I0000734
        primary key (BOUNCED_DATE, ITEM_SERIAL_NUMBER, ACCOUNT_NUMBER)
);

