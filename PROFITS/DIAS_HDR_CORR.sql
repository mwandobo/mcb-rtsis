create table DIAS_HDR_CORR
(
    CREATION_DATE DATE    not null,
    FILE_SN       INTEGER not null,
    TOTAL_RECS    INTEGER,
    ABA           INTEGER,
    TIMESTMP      DATE,
    constraint IXU_CP_059
        primary key (CREATION_DATE, FILE_SN)
);

