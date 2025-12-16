create table PENSIONER_TERM_DEPOS
(
    DEP_TERM_DEPOSIT DECIMAL(11) not null,
    PROJECT          SMALLINT    not null,
    DEP_ACC_FROM     DECIMAL(11) not null,
    DURATION         SMALLINT,
    COUNT            SMALLINT,
    AMOUNT           DECIMAL(15, 2),
    INSERT_DATE      DATE,
    FINAL_DATE       DATE,
    UPD_DATE         DATE,
    TIMESTMP         TIMESTAMP(6),
    STAUS            CHAR(1),
    ENTRY_STATUS     CHAR(1),
    UPD_USER         CHAR(8),
    INSERT_USER      CHAR(8),
    constraint IXU_DEP_139
        primary key (DEP_TERM_DEPOSIT, PROJECT, DEP_ACC_FROM)
);

