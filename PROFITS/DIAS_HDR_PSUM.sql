create table DIAS_HDR_PSUM
(
    CREATION_DATE DATE    not null,
    FILE_SN       INTEGER not null,
    TOTAL_RECS    INTEGER,
    ABA           INTEGER,
    TIMESTMP      DATE,
    constraint IXU_CP_060
        primary key (CREATION_DATE, FILE_SN)
);

