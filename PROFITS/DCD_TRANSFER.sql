create table DCD_TRANSFER
(
    CODE         CHAR(8)      not null,
    TMPSTAMP     TIMESTAMP(6) not null,
    TYPE0        CHAR(2)      not null,
    ENTRY_STATUS CHAR(1),
    STATUS0      CHAR(1),
    PASSWORD     CHAR(26),
    constraint IXU_DEF_062
        primary key (CODE, TMPSTAMP, TYPE0)
);

