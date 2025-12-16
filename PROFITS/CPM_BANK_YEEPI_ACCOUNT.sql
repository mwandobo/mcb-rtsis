create table CPM_BANK_YEEPI_ACCOUNT
(
    PROVIDER_ACCOUNT      VARCHAR(40) not null
        constraint ICPM0001
            primary key,
    STATUS                CHAR(1),
    TRX_USR               CHAR(8),
    TMSTAMP               TIMESTAMP(6),
    FK_SYSTEMIC_BANK_HEAD CHAR(5),
    FK_SYSTEMIC_BANK_NUM  INTEGER,
    FK_YEEPI_BANK_HEAD    CHAR(5),
    FK_YEEPI_BANK_NUM     INTEGER
);

create unique index ICPM0420
    on CPM_BANK_YEEPI_ACCOUNT (FK_SYSTEMIC_BANK_HEAD, FK_SYSTEMIC_BANK_NUM);

create unique index ICPM0580
    on CPM_BANK_YEEPI_ACCOUNT (FK_YEEPI_BANK_HEAD, FK_YEEPI_BANK_NUM);

