create table DCD_TRNS_RULE_ANAL
(
    CODE             CHAR(8)      not null,
    ID               DECIMAL(12)  not null,
    PRFT_SYSTEM      SMALLINT     not null,
    TMPSTAMP         TIMESTAMP(6) not null,
    STATUS0          CHAR(1),
    PASSWORD         CHAR(26),
    FULL_DESCRIPTION VARCHAR(2048),
    EXTENDED_DESC    VARCHAR(2048),
    constraint IXU_DEF_075
        primary key (CODE, ID, PRFT_SYSTEM, TMPSTAMP)
);

