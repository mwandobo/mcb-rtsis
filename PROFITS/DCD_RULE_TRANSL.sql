create table DCD_RULE_TRANSL
(
    LANGUAGE_ID      INTEGER     not null,
    PRFT_SYSTEM      SMALLINT    not null,
    RULE_ID          DECIMAL(12) not null,
    RULE_DESCRIPTION CHAR(50),
    FULL_DESCRIPTION VARCHAR(2048),
    EXTENDED_DESC    VARCHAR(2048),
    constraint IXP_DCD_001
        primary key (LANGUAGE_ID, PRFT_SYSTEM, RULE_ID)
);

