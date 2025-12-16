create table CUSTOMER_CHANNEL
(
    FK_DISTR_CHANNEID  INTEGER not null,
    FK_CUSTOMERCUST_ID INTEGER not null,
    MAX_ATTEMPTS       SMALLINT,
    LOGIN_ATTEMPS      SMALLINT,
    PIN                DECIMAL(13),
    OPENING_DATE       DATE,
    TIMESTAMP          TIMESTAMP(6),
    LAST_UPDATE_DATE   DATE,
    LOGIN_STATUS       CHAR(1),
    ENTRY_STATUS       CHAR(1),
    PREFERRED_LANG     CHAR(2),
    FK_USRCODE         CHAR(8),
    DETAIL_1           CHAR(10),
    constraint IXU_CIS_179
        primary key (FK_DISTR_CHANNEID, FK_CUSTOMERCUST_ID)
);

