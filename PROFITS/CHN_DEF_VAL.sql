create table CHN_DEF_VAL
(
    GROUP_ID   INTEGER  not null,
    ATTR_VALUE CHAR(50) not null,
    ATTR_NAME  CHAR(50) not null,
    constraint PK_CHN_DEF_VAL
        primary key (ATTR_NAME, ATTR_VALUE, GROUP_ID)
);

