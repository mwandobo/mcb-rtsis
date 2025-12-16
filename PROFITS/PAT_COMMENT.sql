create table PAT_COMMENT
(
    COMMENT_ID DECIMAL(10) not null,
    OWNER_TYPE CHAR(2)     not null,
    TEXT       CHAR(240)   not null,
    constraint PATCMPK1
        primary key (OWNER_TYPE, COMMENT_ID)
);

