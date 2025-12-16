create table COS_CACHED_SHARES
(
    SHARE_ID         DECIMAL(10) not null,
    HASH_CODE        CHAR(29)    not null,
    TRX_CODE         INTEGER,
    MEMBER_ID        DECIMAL(10),
    ACTION_INDICATOR CHAR(1),
    TRX_USR          CHAR(8),
    CUSTOMER_ID      INTEGER,
    constraint IXU_CP_118
        primary key (SHARE_ID, HASH_CODE)
);

