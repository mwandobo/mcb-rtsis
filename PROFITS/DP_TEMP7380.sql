create table DP_TEMP7380
(
    PROGRAM_ID    CHAR(5) not null,
    CURR_SH_DESC  CHAR(5) not null,
    SN            INTEGER not null,
    DR_AMOUNT     DECIMAL(15, 2),
    DR_AMOUNT_DOM DECIMAL(15, 2),
    CR_AMOUNT     DECIMAL(15, 2),
    CR_AMOUNT_DOM DECIMAL(15, 2),
    constraint IXU_REP_048
        primary key (PROGRAM_ID, CURR_SH_DESC, SN)
);

