create table MSG_RECIPIENTS
(
    FK_SQL_REP_ID        DECIMAL(12) not null,
    FK_FIELD_MAP_ID      SMALLINT    not null,
    FK_RECIP_REP_ID      DECIMAL(12) not null,
    FK_RECP_SQL_REP_ID   DECIMAL(12),
    FK_RECP_FIELD_MAP_ID SMALLINT,
    GENERIC_RECIPIENT    VARCHAR(100),
    constraint IXM_RCP_001
        primary key (FK_SQL_REP_ID, FK_FIELD_MAP_ID, FK_RECIP_REP_ID)
);

