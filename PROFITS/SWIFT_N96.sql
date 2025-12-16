create table SWIFT_N96
(
    TRX_REF_NO_20      CHAR(16),
    RELATED_REF_21     CHAR(16),
    MSG_TYPE           CHAR(2),
    MESSAGE_TYPE       CHAR(20),
    MSG_CATEGORY       CHAR(1)  not null,
    SENDER_BIC         CHAR(12),
    RECEIVER_BIC       CHAR(12),
    PRIORITY_CODE      CHAR(2),
    ORIG_MT_TAG_11     CHAR(5)  not null,
    ORIG_MT_OPT_11     CHAR(1)  not null,
    ORIG_MT_TYPE_11    CHAR(3)  not null,
    ORIG_MT_DATE_11    DATE     not null,
    ORIG_MT_SESSION_11 VARCHAR(40),
    ANSWER_TAG_76      CHAR(5)  not null,
    NARRATIVE_TAG_77   CHAR(5)  not null,
    NARRATIVE_OPT_77   CHAR(1)  not null,
    ORIG_MT_TAG_79     CHAR(5)  not null,
    TRX_REF_NO_20_XML  VARCHAR(140),
    RELATED_REF_21_XML VARCHAR(140),
    FK_SWMSG_PRFTREFNO CHAR(16) not null
        constraint PK_SWIFT_N96
            primary key,
    RESPONSE_FLAG      CHAR(1)
);

