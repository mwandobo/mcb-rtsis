create table DCD_TRNS_RULE_TRSL
(
    CODE             CHAR(8)      not null,
    LANGUAGE_ID      INTEGER      not null,
    PRFT_SYSTEM      SMALLINT     not null,
    RULE_ID          DECIMAL(12)  not null,
    TMPSTAMP         TIMESTAMP(6) not null,
    STATUS0          CHAR(1),
    PASSWORD         CHAR(26),
    RULE_DESCRIPTION CHAR(50),
    EXTENDED_DESC    VARCHAR(2048),
    FULL_DESCRIPTION VARCHAR(2048),
    constraint IXU_DEF_077
        primary key (CODE, LANGUAGE_ID, PRFT_SYSTEM, RULE_ID, TMPSTAMP)
);

