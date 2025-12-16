create table SWIFT_76X
(
    FK_SWMSG_PRFTREFNO CHAR(16) not null,
    FK_ACTUAL_DOCUMPRF SMALLINT,
    AMENDEMENT_NO      SMALLINT,
    FK_ACTUAL_DOCUMSER SMALLINT,
    FK_UNITCODE        INTEGER,
    FK_ACTUAL_DOCUMID  DECIMAL(13),
    ISSUANCE_DATE      DATE,
    AMENDEMENT_REQ_DT  DATE,
    CHK_26E            CHAR(1),
    CHK_SWC            CHAR(1),
    CHK_31C            CHAR(1),
    CHK_30             CHAR(1),
    CHK_23             CHAR(1),
    CHK_72             CHAR(1),
    CHK_21             CHAR(1),
    CHK_20             CHAR(1),
    CHK_40C            CHAR(1),
    PRIORITY_CODE      CHAR(2),
    APP_RULES_HEADER   CHAR(4),
    SWIFT_CODE         CHAR(12),
    FUTHER_INFO        CHAR(16),
    RELATED_REFERENCE  CHAR(16),
    TRANSACTION_REF_NO CHAR(16),
    APP_RULES_TXT      CHAR(35),
    SENDER_RECVER_INF  CHAR(210),
    TMSTAMP            TIMESTAMP(6),
    SWIFT_TYPE         CHAR(3),
    SEND_USERCODE      CHAR(4),
    SEND_TIME          TIMESTAMP(6),
    CFRM_USERCODE      CHAR(4),
    CFRM_DATE          DATE,
    ENTRY_STATUS       CHAR(1)
);

create unique index IXU_SWI_027
    on SWIFT_76X (FK_SWMSG_PRFTREFNO);

alter table SWIFT_76X
    add constraint SW76
        primary key (FK_SWMSG_PRFTREFNO);

