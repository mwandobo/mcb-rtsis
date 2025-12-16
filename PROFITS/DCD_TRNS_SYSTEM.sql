create table DCD_TRNS_SYSTEM
(
    CODE        CHAR(8)      not null,
    ID          DECIMAL(12)  not null,
    TMPSTAMP    TIMESTAMP(6) not null,
    STATUS0     CHAR(1),
    PASSWORD    CHAR(26),
    SYSTEM_DESC CHAR(40),
    constraint IXU_DEF_078
        primary key (CODE, ID, TMPSTAMP)
);

