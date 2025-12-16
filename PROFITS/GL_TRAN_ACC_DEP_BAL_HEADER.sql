create table GL_TRAN_ACC_DEP_BAL_HEADER
(
    ARTICLE_SN           DECIMAL(15) not null
        constraint IXU_TADBH_001
            primary key,
    DESCRIPTION          VARCHAR(240),
    DATE_FROM            DATE,
    DATE_TO              DATE,
    FILE_NAME            VARCHAR(240),
    FREQUENCY            DECIMAL(2),
    JUSTIFICATION        VARCHAR(40),
    LAST_RUN_DATE        TIMESTAMP(6),
    LAST_RUN_TMPSTAMP    TIMESTAMP(6),
    PERCENTAGE           DECIMAL(3),
    AMOUNT_TYPE          DECIMAL(1),
    SUBSYSTEM            CHAR(2),
    FK_CURR_GROUP_ID     CHAR(4),
    FK_UNIT_GROUP_ID     DECIMAL(5),
    FK_GLG_ACCOUNTACCO   CHAR(21),
    FK_DEP_ACCOUNT_NO    DECIMAL(11),
    FK_PROFITS_ACCOUNT   CHAR(40),
    PER_TRAN_AG_UNIT_USR CHAR(1),
    TRN_USR              CHAR(8),
    TRN_UNIT             DECIMAL(5),
    TRN_TMPSTAMP         TIMESTAMP(6),
    EXECUTION_SEQ        DECIMAL(2) default 1
);

