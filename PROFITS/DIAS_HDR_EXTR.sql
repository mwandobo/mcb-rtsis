create table DIAS_HDR_EXTR
(
    CREATION_DATE DATE    not null,
    FILE_SN       INTEGER not null,
    TOTAL_RECS    INTEGER,
    ABA           INTEGER,
    TIMESTMP      DATE,
    constraint IXU_CP_086
        primary key (CREATION_DATE, FILE_SN)
);

