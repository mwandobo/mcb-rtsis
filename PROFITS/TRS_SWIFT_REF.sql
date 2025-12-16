create table TRS_SWIFT_REF
(
    DEAL_NO           INTEGER  not null,
    DEAL_TYPE         CHAR(2)  not null,
    PRFT_REF_NO       CHAR(16) not null,
    RELATED_REFERENCE CHAR(16) not null,
    TMSTAMP           TIMESTAMP(6),
    MESSAGE_TYPE      CHAR(20),
    constraint PK_TRS_SWIFT_REF
        primary key (DEAL_NO, DEAL_TYPE, PRFT_REF_NO, RELATED_REFERENCE)
);

