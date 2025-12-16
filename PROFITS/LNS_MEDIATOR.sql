create table LNS_MEDIATOR
(
    AGREEMENT_NO       INTEGER,
    NAME               CHAR(80),
    ENTRY_STATUS       CHAR(1),
    FK_GENERIC_DETAFK  CHAR(5),
    FK_GENERIC_DETASER INTEGER,
    DEP_ACC_NUMBER     DECIMAL(11),
    CUST_ID            INTEGER,
    C_DIGIT            SMALLINT,
    ADD_INFO           VARCHAR(2048)
);

create unique index IXU_LOA_063
    on LNS_MEDIATOR (AGREEMENT_NO);

