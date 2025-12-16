create table GENERIC_DETAIL_INFO
(
    GEN_DESCRIPTION                CHAR(40),
    GEN_NUMBER                     INTEGER,
    GEN_INFO                       CHAR(5),
    GENERIC_FLAG                   CHAR(1),
    LATIN_DESCRIPTION              VARCHAR(40),
    TMSTAMP                        TIMESTAMP(6),
    FK_GENERIC_DETAFK_GENERIC_HEAD CHAR(5) not null,
    FK_GENERIC_DETASERIAL_NUM      INTEGER not null,
    constraint I0000673
        primary key (FK_GENERIC_DETAFK_GENERIC_HEAD, FK_GENERIC_DETASERIAL_NUM)
);

