create table GL_TRAN_ACC_DEP_BAL_DETAIL
(
    FK_ARTICLE_SN      DECIMAL(15) not null,
    SN                 DECIMAL(15) not null,
    SUBSYSTEM          CHAR(2),
    POSTING_UNIT       CHAR(1),
    SPECIFIC_UNIT      DECIMAL(15),
    PARAMETRIC         CHAR(1),
    PERCENTAGE         DECIMAL(15),
    ROUNDING           CHAR(1),
    FK_DEP_ACCOUNT_NO  DECIMAL(15),
    FK_PROFITS_ACCOUNT CHAR(40),
    TARGET_CURRENCY    CHAR(1),
    FK_GLG_ACCOUNTACCO CHAR(21),
    TRN_USR            CHAR(8),
    TRN_UNIT           DECIMAL(15),
    TRN_TMPSTAMP       TIMESTAMP(6),
    constraint IXU_TADBD_001
        primary key (FK_ARTICLE_SN, SN)
);

