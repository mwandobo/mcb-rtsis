create table DCD_TRNS_EXTST_TRA
(
    CODE            CHAR(8)      not null,
    ID              DECIMAL(12)  not null,
    LANGUAGE_USED   INTEGER      not null,
    PRFT_SYSTEM     SMALLINT     not null,
    TMPSTAMP        TIMESTAMP(6) not null,
    STATUS0         CHAR(1),
    PASSWORD        CHAR(26),
    EXIT_STATE_DESC CHAR(40),
    ACTUAL_MESSAGE  VARCHAR(2048),
    constraint IXU_DEF_064
        primary key (CODE, ID, LANGUAGE_USED, PRFT_SYSTEM, TMPSTAMP)
);

