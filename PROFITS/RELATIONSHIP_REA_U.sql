create table RELATIONSHIP_REA_U
(
    FKCUST_HAS_AS_FIRS INTEGER      not null,
    FKCUST_HAS_AS_SECO INTEGER      not null,
    FK_RELATIONSHIPFK  CHAR(12)     not null,
    REASON_TYPE        CHAR(1)      not null,
    REASON_ACCOUNT     CHAR(20)     not null,
    COMMENTS_1         CHAR(80)     not null,
    COMMENTS_2         CHAR(80)     not null,
    TMSTAMP            TIMESTAMP(6) not null,
    constraint IXU_CIU_052
        primary key (FK_RELATIONSHIPFK, REASON_ACCOUNT, REASON_TYPE, FKCUST_HAS_AS_SECO, FKCUST_HAS_AS_FIRS)
);

