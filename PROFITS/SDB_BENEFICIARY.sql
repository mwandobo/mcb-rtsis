create table SDB_BENEFICIARY
(
    TMSTAMP            TIMESTAMP(6) not null,
    MAIN_BENEF_FLG     CHAR(1),
    BENEF_STATUS       CHAR(1),
    REMOVAL_DT         DATE         not null,
    FKSDB_RENTAL_UNIT  INTEGER      not null,
    FKSDB_RENTAL_SN    INTEGER      not null,
    FKSDB_RENTAL_DATE  DATE         not null,
    FK_CUSTOMERCUST_ID INTEGER,
    constraint IXU_DEP_107
        primary key (FKSDB_RENTAL_UNIT, FKSDB_RENTAL_SN, FKSDB_RENTAL_DATE, TMSTAMP)
);

