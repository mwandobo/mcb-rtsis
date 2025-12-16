create table MAND_OPT_DCD
(
    BANK_CODE    SMALLINT not null,
    SEC_WIN_CODE CHAR(8)  not null,
    ACT_CODE     CHAR(1)  not null,
    DCD_SYS      SMALLINT,
    DCD_RULE     DECIMAL(12),
    constraint IXU_DCD_044
        primary key (ACT_CODE, SEC_WIN_CODE, BANK_CODE)
);

