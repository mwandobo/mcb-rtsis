create table COS_PENDING_DATA
(
    TRX_CODE    INTEGER not null,
    SHARE_ID    INTEGER not null,
    MEMBER_ID   INTEGER,
    ACTION_CODE CHAR(1),
    TRX_USR     CHAR(8),
    constraint IXU_CP_074
        primary key (TRX_CODE, SHARE_ID)
);

