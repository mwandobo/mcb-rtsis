create table TMP_SWIFT_RUN
(
    SUBTAG         SMALLINT     not null,
    TAG            CHAR(10)     not null,
    MESSAGE_TYPE   CHAR(20)     not null,
    TMSTAMP        TIMESTAMP(6) not null,
    REPETATIVE_REF INTEGER,
    REF_FLD_FLAG   CHAR(1),
    FIELD_TYPE     CHAR(2),
    DATA           CHAR(100),
    ERROR_COMMENTS VARCHAR(100),
    constraint IXU_REP_155
        primary key (SUBTAG, TAG, MESSAGE_TYPE, TMSTAMP)
);

