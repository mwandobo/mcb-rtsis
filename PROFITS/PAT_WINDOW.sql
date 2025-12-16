create table PAT_WINDOW
(
    ID              DECIMAL(10)  not null
        constraint PATWDPK1
            primary key,
    WINDOW_NAME     CHAR(64)     not null,
    TITLE           CHAR(100)    not null,
    TYPE0           SMALLINT     not null,
    LAST_CHANGED    TIMESTAMP(6) not null,
    STATUS          CHAR(1)      not null,
    FK_PAT_PSTEPSID DECIMAL(10),
    TITLE_GR        CHAR(100)
);

create unique index PATWDI1
    on PAT_WINDOW (FK_PAT_PSTEPSID);

