create table SWIFT_N92_DTL
(
    SN                 SMALLINT not null,
    TAG_DTL            CHAR(5)  not null,
    NARRATIVE_LINE     VARCHAR(100),
    FK_SWMSG_PRFTREFNO CHAR(16) not null,
    constraint PK_SWIFT_N92_DTL
        primary key (FK_SWMSG_PRFTREFNO, SN)
);

