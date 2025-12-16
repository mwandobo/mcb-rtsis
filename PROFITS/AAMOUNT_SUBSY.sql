create table AAMOUNT_SUBSY
(
    SYBSYSTEM_ID INTEGER  not null,
    AMOUNT_SNO   SMALLINT not null,
    DESCRIPTION  VARCHAR(40),
    COMMENTS     VARCHAR(40),
    constraint IXU_AAM_000
        primary key (SYBSYSTEM_ID, AMOUNT_SNO)
);

