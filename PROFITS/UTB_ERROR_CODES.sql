create table UTB_ERROR_CODES
(
    CODE        SMALLINT not null,
    SERVICE     INTEGER  not null,
    CODE_FLAG   CHAR(1),
    DESCRIPTION CHAR(30),
    constraint IXU_CP_106
        primary key (CODE, SERVICE)
);

