create table DCD_TRNS_MODEL_INF
(
    CODE        CHAR(8)      not null,
    MODEL_ID    DECIMAL(12)  not null,
    TMPSTAMP    TIMESTAMP(6) not null,
    PRFT_SYSTEM SMALLINT,
    STATUS0     CHAR(1),
    PASSWORD    CHAR(26),
    MODEL_NAME  CHAR(80),
    constraint IXU_DEF_068
        primary key (CODE, MODEL_ID, TMPSTAMP)
);

