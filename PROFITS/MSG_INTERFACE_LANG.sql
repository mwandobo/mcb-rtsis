create table MSG_INTERFACE_LANG
(
    ID          VARCHAR(2)  not null
        constraint IXM_INL_001
            primary key,
    CULTURE     VARCHAR(10) not null,
    DESCRIPTION VARCHAR(20) not null
);

