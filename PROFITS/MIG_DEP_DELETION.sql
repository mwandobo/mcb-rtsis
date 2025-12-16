create table MIG_DEP_DELETION
(
    ACTION            CHAR(1)     not null,
    PROCESS_DATE      DATE        not null,
    REPRESENTATIVE_SN SMALLINT    not null,
    DEPACC_NO         DECIMAL(11) not null,
    UNIT_CODE         INTEGER     not null,
    TIMESTMP          DATE,
    PROCESS_STATUS    CHAR(1),
    PROCESS_COMMENT   CHAR(250),
    constraint IXU_MIG_033
        primary key (ACTION, PROCESS_DATE, REPRESENTATIVE_SN, DEPACC_NO, UNIT_CODE)
);

