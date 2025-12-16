create table CGN_VIEW_ACCESS
(
    VIEW_SN         DECIMAL(10) not null,
    VIEW_PROFILE_ID CHAR(8)     not null,
    constraint PK_VIEW_ACCESS
        primary key (VIEW_SN, VIEW_PROFILE_ID)
);

