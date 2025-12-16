create table GLG_TMP_CNFTAX_TRN
(
    UNIT_CODE      INTEGER  not null,
    TAX_TYPE       CHAR(4)  not null,
    GL_TRN_DATE    DATE     not null,
    DOC_ID         CHAR(4)  not null,
    DOC_SER        CHAR(2)  not null,
    DOC_NUM        INTEGER  not null,
    LINE_NUM       SMALLINT not null,
    ACC_CNFTAX     CHAR(1)  not null,
    ACCOUNT_ID     CHAR(21) not null,
    SUBSYSTEM      CHAR(2),
    AMOUNT         DECIMAL(15, 2),
    TAX_ID         CHAR(4)  not null,
    RATE           DECIMAL(5, 2),
    MVMNT_TYPE     CHAR(1),
    TOLERANCE_AMNT INTEGER,
    CURR_ID        INTEGER  not null,
    UNIT_NAME      VARCHAR(40),
    constraint IXU_GLG_106
        primary key (LINE_NUM, DOC_NUM, DOC_SER, DOC_ID, GL_TRN_DATE, TAX_TYPE, UNIT_CODE)
);

