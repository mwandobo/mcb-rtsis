create table SDB_USE
(
    USE_DATE           TIMESTAMP(6) not null,
    USE_COMMENTS       CHAR(40),
    FKSDB_RENTAL_UNIT  INTEGER      not null,
    FKSDB_RENTAL_SN    INTEGER      not null,
    FKSDB_RENTAL_DATE  DATE         not null,
    FK_CUSTOMERCUST_ID INTEGER,
    USE_DETAILS        VARCHAR(240),
    constraint IXU_DEP_111
        primary key (FKSDB_RENTAL_UNIT, FKSDB_RENTAL_SN, FKSDB_RENTAL_DATE, USE_DATE)
);

