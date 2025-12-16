create table GLG_CALLOC_SHEET
(
    COST_ALLOC_NUM   INTEGER not null
        constraint IXU_GLG_011
            primary key,
    TMSTAMP          TIMESTAMP(6),
    EXEC_TYPE        CHAR(1),
    USAGE_STATUS     CHAR(1),
    ENTRY_STATUS     CHAR(1),
    DEBIT_CREDIT_IND CHAR(1),
    FK_GLG_DOC_SER   CHAR(2),
    FK_GLG_JUSTIFY   CHAR(4),
    FK_GLG_DOC_ID    CHAR(4),
    FK_GL_ACCOUNT    CHAR(21),
    FK_AL_ACCOUNT    CHAR(21),
    DESCRIPTION      CHAR(40)
);

