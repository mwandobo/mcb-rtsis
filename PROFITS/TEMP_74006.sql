create table TEMP_74006
(
    ACCOUNT_SER_NUM DECIMAL(11) not null,
    RECORD_SN       DECIMAL(15) not null,
    PROCESS_FLG     CHAR(2),
    constraint PK_TEMP_74006
        primary key (RECORD_SN, ACCOUNT_SER_NUM)
);

comment on column TEMP_74006.PROCESS_FLG is 'PROCESS_FLG    0 - Not Processed  1 - Processed';

