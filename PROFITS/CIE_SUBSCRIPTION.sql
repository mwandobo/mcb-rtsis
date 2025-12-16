create table CIE_SUBSCRIPTION
(
    CIE_CUSTOMER         INTEGER not null
        constraint IXU_DEF_143
            primary key,
    TRIES_COUNT          SMALLINT,
    FK_PROFILE_ID        SMALLINT,
    PREFERRED_LANG       INTEGER,
    FK_CUST_ID           INTEGER,
    PSW_LAST_CHANGE_DATE DATE,
    CREATE_DATE          DATE,
    TMSTAMP              TIMESTAMP(6),
    FORCE_CHANGE_FLAG    CHAR(1),
    ENTRY_STATUS         CHAR(1),
    CSS_SCHEME           CHAR(1),
    LOGIN_STATUS         CHAR(1),
    STATUS               CHAR(1),
    ALIAS_CIE_CUSTOMER   CHAR(30),
    INIT_PASSWORD        CHAR(60),
    PASSWORD             CHAR(88)
);

