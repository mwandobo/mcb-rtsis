create table XPAT_WINDOW
(
    WINDOW_NAME     CHAR(64)    not null,
    ID              DECIMAL(10) not null
        constraint PATXWDPK
            primary key,
    TITLE           CHAR(100),
    TYPE0           SMALLINT,
    LAST_CHANGED    TIMESTAMP(6),
    STATUS          CHAR(1),
    TITLE_GR        CHAR(100),
    FK_XPAT_PSTEPID DECIMAL(10),
    FK_PAT_WINDOWID DECIMAL(10)
);

comment on column XPAT_WINDOW.ID is 'The unique sequential number that also is base for creation of the unique keys of the fields.';

comment on column XPAT_WINDOW.TYPE0 is 'TYPE OF WINDOW: PRIMARY, DIALOG...';

create unique index PATXWDI1
    on XPAT_WINDOW (FK_XPAT_PSTEPID);

create unique index PATXWDI2
    on XPAT_WINDOW (FK_PAT_WINDOWID);

