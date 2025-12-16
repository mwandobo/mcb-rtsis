create table COS_SHARE_PRC_HIST
(
    DIVIDENT_DATE  DATE        not null,
    SHARE_ID       DECIMAL(10) not null,
    MEMBER_ID      DECIMAL(10),
    DIVIDENT_PRICE DECIMAL(15, 2),
    constraint IXU_CP_077
        primary key (DIVIDENT_DATE, SHARE_ID)
);

