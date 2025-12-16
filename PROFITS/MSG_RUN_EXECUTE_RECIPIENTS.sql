create table MSG_RUN_EXECUTE_RECIPIENTS
(
    FK_RUN_EXEC_RSLT_ID DECIMAL(12)  not null,
    TEMP_RECIP          VARCHAR(200) not null,
    TEMP_RECIP_LANG     SMALLINT default 1,
    constraint IXM_RET_001
        primary key (FK_RUN_EXEC_RSLT_ID, TEMP_RECIP)
);

