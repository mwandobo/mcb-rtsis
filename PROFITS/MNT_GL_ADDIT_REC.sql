create table MNT_GL_ADDIT_REC
(
    TRX_DATE      DATE     not null,
    TRX_UNIT      INTEGER  not null,
    TRX_USER      CHAR(8)  not null,
    TRX_USR_SN    INTEGER  not null,
    GRP_SUBSCRIPT SMALLINT not null,
    LOAN_STATUS   CHAR(1),
    LOAN_CLASS    CHAR(1),
    FK_CCODE_GH   CHAR(5),
    FK_CCODE_GD   INTEGER,
    FK_CLOAN_GH   CHAR(5),
    FK_CLOAN_GD   INTEGER,
    FK_FINSC_GH   CHAR(5),
    FK_FINSC_GD   INTEGER,
    LOAN_DURATION INTEGER,
    constraint MNT_GL_PK
        primary key (GRP_SUBSCRIPT, TRX_USR_SN, TRX_USER, TRX_UNIT, TRX_DATE)
);

create unique index I0000834
    on MNT_GL_ADDIT_REC (FK_FINSC_GH, FK_FINSC_GD);

create unique index I0000836
    on MNT_GL_ADDIT_REC (FK_CLOAN_GH, FK_CLOAN_GD);

create unique index I0000838
    on MNT_GL_ADDIT_REC (FK_CCODE_GH, FK_CCODE_GD);

