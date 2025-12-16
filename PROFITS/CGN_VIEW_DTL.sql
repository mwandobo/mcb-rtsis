create table CGN_VIEW_DTL
(
    VIEW_DTL_SN      SMALLINT     not null,
    VIEW_UNION_INTER CHAR(3),
    TMSTAMP          TIMESTAMP(6) not null,
    FK_VIEW_SN       DECIMAL(10)  not null,
    FK_SCRIPT_SN     DECIMAL(10),
    FK_GLG_ACCOUNT   CHAR(21),
    constraint PK_VIEW_DTL
        primary key (FK_VIEW_SN, VIEW_DTL_SN)
);

