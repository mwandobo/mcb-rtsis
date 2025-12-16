create table DCD_RULE_ANALYSIS
(
    ID               DECIMAL(12) not null,
    PRFT_SYSTEM      SMALLINT    not null,
    EXTENDED_DESC    VARCHAR(2048),
    FULL_DESCRIPTION VARCHAR(2048),
    constraint IXU_DEF_119
        primary key (ID, PRFT_SYSTEM)
);

