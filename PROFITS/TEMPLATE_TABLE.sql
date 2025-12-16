create table TEMPLATE_TABLE
(
    TEMPLATE_CODE                  CHAR(8)      not null
        constraint IXU_TMP_001
            primary key,
    TMSTAMP                        TIMESTAMP(6) not null,
    TEST2                          CHAR(2)      not null,
    TEMPLATE_STATUS                CHAR(1)      not null,
    TEST1                          SMALLINT     not null,
    TEMPLATE_DESCRIPTION           VARCHAR(40),
    FK_CURRENCYID_CURRENCY         INTEGER,
    FK_GENERIC_DETAFK_GENERIC_HEAD CHAR(5),
    FK_GENERIC_DETASERIAL_NUM      INTEGER,
    FK_CURRENCYID_CURR             INTEGER,
    FK_GENERIC_DETAFK              CHAR(5),
    FK_GENERIC_DETASER             INTEGER,
    TEMPLATE_DESCRIPTI             CHAR(40)
);

