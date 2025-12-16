create table CMS_LIMIT_PROFILES
(
    PROFILE_ID    CHAR(8)  not null,
    ENTRY_STATUS  CHAR(1),
    FK_LIMIT_HDCD CHAR(15) not null,
    constraint PK_LIMIT_PROFILES
        primary key (FK_LIMIT_HDCD, PROFILE_ID)
);

