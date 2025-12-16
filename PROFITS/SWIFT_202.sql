create table SWIFT_202
(
    FK_SWMSG_PRFTREFNO CHAR(16),
    CHK_32A            CHAR(1),
    CHK_53A            CHAR(1),
    CHK_57A            CHAR(1),
    CHK_72             CHAR(1),
    CHK_58A            CHAR(1),
    CHK_21             CHAR(1),
    CHK_52A            CHAR(1),
    CHK_54A            CHAR(1),
    CHK_56A            CHAR(1),
    CHECK_13C          CHAR(1),
    CHK_20             CHAR(1),
    PRIORITY_CODE      CHAR(2),
    RECEIVER_BIC       CHAR(12),
    SENDER_BIC         CHAR(12),
    RELATED_REF_21     CHAR(16),
    TRX_REF_NO_20      CHAR(16),
    TIME_INDICATIO_13C CHAR(20),
    DATE_CUR_AMN_32A   CHAR(26),
    ORDERING_INSTI_52A VARCHAR(183),
    BENEF_INSTIT_58A   VARCHAR(183),
    SNDRS_CORR_53A     VARCHAR(183),
    RCVRS_CORR_54A     VARCHAR(183),
    INTERMEDIARY_56A   VARCHAR(183),
    ACC_WITH_INSTI_57A VARCHAR(183),
    SNDR_TO_RCVR_72    VARCHAR(215)
);

create unique index IXU_SWI_022
    on SWIFT_202 (FK_SWMSG_PRFTREFNO);

create unique index SKEY_MT202_REF
    on SWIFT_202 (SENDER_BIC, RELATED_REF_21);

