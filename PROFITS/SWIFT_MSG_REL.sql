create table SWIFT_MSG_REL
(
    TRX_REF_NO_20     CHAR(16) not null
        constraint IXU_SWI_007
            primary key,
    TMSTAMP           TIMESTAMP(6),
    REL_TYPE          CHAR(1),
    REL_TRX_REF_NO_20 CHAR(16),
    MESSAGE_TYPE      CHAR(20)
);

