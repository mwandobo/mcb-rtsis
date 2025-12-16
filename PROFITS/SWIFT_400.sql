create table SWIFT_400
(
    FK_SWMSG_PRFTREFNO CHAR(16),
    CHK_58A            CHAR(1),
    CHK_54A            CHAR(1),
    CHK_57A            CHAR(1),
    CHK_53A            CHAR(1),
    CHK_52A            CHAR(1),
    CHK_73             CHAR(1),
    CHK_72             CHAR(1),
    CHK_71B            CHAR(1),
    CHK_20             CHAR(1),
    CHK_21             CHAR(1),
    CHK_32A            CHAR(1),
    CHK_33A            CHAR(1),
    PRIORITY_CODE      CHAR(2),
    SENDER_BIC         CHAR(12),
    RECEIVER_BIC       CHAR(12),
    RELATED_REF_21     CHAR(16),
    TRX_REF_NO_20      CHAR(16),
    PROCEEDS_REMIT_33A CHAR(26),
    AMN_COLLECTED_32A  CHAR(26),
    RCVRS_CORR_54A     VARCHAR(183),
    SNDRS_CORR_53A     VARCHAR(183),
    BENEF_BANK_58A     VARCHAR(183),
    ORDERING_BANK_52A  VARCHAR(183),
    ACC_WITH_INSTI_57A VARCHAR(183),
    SNDR_TO_RCVR_72    VARCHAR(215),
    DET_OF_CHARGES_71B VARCHAR(215),
    DET_OF_AMNS_73     VARCHAR(215)
);

create unique index IXU_SWI_026
    on SWIFT_400 (FK_SWMSG_PRFTREFNO);

