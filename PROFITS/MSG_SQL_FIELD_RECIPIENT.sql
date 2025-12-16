create table MSG_SQL_FIELD_RECIPIENT
(
    FK_FIELD_MAP_ID   SMALLINT    not null,
    FK_SQL_REP_ID     DECIMAL(12) not null,
    FK_RECIP_REPOS_ID DECIMAL(12) not null,
    GROUPING_FLG      SMALLINT default 0,
    LIST_FLG          SMALLINT default 0,
    constraint IXM_SFR_001
        primary key (FK_FIELD_MAP_ID, FK_SQL_REP_ID, FK_RECIP_REPOS_ID)
);

