create table SWIFT_200
(
    FK_SWMSG_PRFTREFNO CHAR(16),
    CHK_53B            CHAR(1),
    CHK_72             CHAR(1),
    CHK_57A            CHAR(1),
    CHK_56A            CHAR(1),
    CHK_20             CHAR(1),
    CHK_32A            CHAR(1),
    SENDER_BIC         CHAR(12),
    RECEIVER_BIC       CHAR(12),
    TRX_REF_NO_20      CHAR(16),
    VALUE_CUR_AMN_32A  CHAR(26),
    SNDRS_CORR_53B     VARCHAR(75),
    ACC_WITH_INSTI_57A VARCHAR(183),
    INTERMEDIARY_56A   VARCHAR(185),
    SNDR_TO_RCVR_IN_72 VARCHAR(215)
);

create unique index IXU_SWI_021
    on SWIFT_200 (FK_SWMSG_PRFTREFNO);

