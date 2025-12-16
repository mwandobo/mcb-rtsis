create table ATM_OTH_PROCESS_CODE
(
    PROTOCOL_ID      SMALLINT not null,
    ISO_CODE         CHAR(6)  not null,
    MTI_CODE         CHAR(6)  not null,
    MNEMONIC_CODE    CHAR(4)  not null,
    INTER_TRANS_CODE SMALLINT,
    REVERSAL_FLAG    CHAR(1)  not null,
    DIAS_FLAG        CHAR(1),
    DESCRIPTION      CHAR(80),
    constraint I0000591
        primary key (REVERSAL_FLAG, MNEMONIC_CODE, MTI_CODE, ISO_CODE, PROTOCOL_ID)
);

