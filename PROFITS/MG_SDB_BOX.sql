create table MG_SDB_BOX
(
    BRANCH            INTEGER not null,
    SDB_CODE          INTEGER not null,
    SDB_TYPE          INTEGER,
    SDB_STATUS        CHAR(1),
    SDB_COMMENTS      CHAR(80),
    RENTAL_DATE       DATE    not null,
    RENTAL_STATUS     CHAR(1),
    MAIN_RENTOR       INTEGER,
    RENTAL_EXPIRATION DATE,
    RENTAL_AMOUNT     DECIMAL(15, 2),
    RENTAL_COMMENTS   CHAR(80),
    RENTOR_2ND        INTEGER,
    MIG_STATUS        CHAR(1),
    ERR_DESCR         CHAR(80),
    PROD_UNIT_SN      INTEGER,
    MIG_DATE          DATE,
    constraint I
        primary key (RENTAL_DATE, SDB_CODE, BRANCH)
);

comment on table MG_SDB_BOX is 'Table used for the migration of Safe Deposit Boxes';

