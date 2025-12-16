create table OFFICIAL_HLD
(
    FK_CURRENCYID_CURR INTEGER,
    DATE_ID            DATE,
    NATIONAL_HLD       CHAR(1),
    DESCRIPTION        VARCHAR(40)
);

create unique index IXU_OFF_000
    on OFFICIAL_HLD (FK_CURRENCYID_CURR, DATE_ID);

