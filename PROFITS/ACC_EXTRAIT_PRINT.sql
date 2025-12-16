create table ACC_EXTRAIT_PRINT
(
    SYSTEM0            SMALLINT    not null,
    TYPE0              SMALLINT    not null,
    UNIT               INTEGER     not null,
    SN                 DECIMAL(16) not null,
    PENDING_EXTR_LINES DECIMAL(10),
    PRINT_DATE         DATE,
    constraint IXU_DEF_146
        primary key (SYSTEM0, TYPE0, UNIT, SN)
);

