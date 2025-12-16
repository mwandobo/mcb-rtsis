create table DCD_TRNS_UPLOAD
(
    CODE     CHAR(8)      not null,
    SN       INTEGER      not null,
    TMPSTAMP TIMESTAMP(6) not null,
    TABLE0   CHAR(50),
    FIELD0   CHAR(50),
    constraint IXU_DEF_127
        primary key (CODE, SN, TMPSTAMP)
);

