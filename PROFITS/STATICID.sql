create table STATICID
(
    LOADMODULE CHAR(8)     not null,
    WINDOW     VARCHAR(35) not null,
    CONTROL    VARCHAR(60) not null,
    CONTROLID  INTEGER     not null,
    constraint PK_IDPERLM
        primary key (CONTROLID, LOADMODULE)
);

create unique index PK_STATICID
    on STATICID (CONTROL, WINDOW, LOADMODULE);

