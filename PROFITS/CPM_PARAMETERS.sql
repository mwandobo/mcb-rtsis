create table CPM_PARAMETERS
(
    PARAMETER_TYPE        CHAR(5) not null,
    PARAMETER_SN          INTEGER not null,
    DESCRIPTION           VARCHAR(200),
    ENTRY_STATUS          CHAR(1),
    TMSTAMP               TIMESTAMP(6),
    FK_SYSTEMIC_BANK_HEAD CHAR(5) not null,
    FK_SYSTEMIC_BANK_NUM  INTEGER not null,
    constraint ICPMPAR01
        primary key (FK_SYSTEMIC_BANK_HEAD, FK_SYSTEMIC_BANK_NUM, PARAMETER_SN, PARAMETER_TYPE)
);

