create table MSG_RECIPIENTS_REPOSITORY
(
    ID                 DECIMAL(12)        not null
        constraint IXM_RRP_001
            primary key,
    LABEL              VARCHAR(60)        not null,
    DESCRIPTION        VARCHAR(255),
    STATUS             SMALLINT default 0,
    STATIC             SMALLINT default 0 not null,
    FK_RECP_SQL_REP_ID DECIMAL(12),
    LANGUAGE_COL       SMALLINT default 0
);

