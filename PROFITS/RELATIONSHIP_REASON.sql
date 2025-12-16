create table RELATIONSHIP_REASON
(
    REASON_TYPE        CHAR(1)      not null,
    REASON_ACCOUNT     CHAR(20)     not null,
    COMMENTS_1         CHAR(80)     not null,
    COMMENTS_2         CHAR(80)     not null,
    TMSTAMP            TIMESTAMP(6) not null,
    FK_RELATIONSHIPFK  CHAR(12)     not null,
    FKCUST_HAS_AS_FIRS INTEGER      not null,
    FKCUST_HAS_AS_SECO INTEGER      not null,
    constraint PKRELACC
        primary key (REASON_TYPE, REASON_ACCOUNT, FK_RELATIONSHIPFK, FKCUST_HAS_AS_FIRS, FKCUST_HAS_AS_SECO)
);

