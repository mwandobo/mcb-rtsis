create table MSG_SQL_REPOSITORY
(
    ID              DECIMAL(12)   not null
        constraint IXM_SRP_001
            primary key,
    DESCRIPTION     VARCHAR(250)  not null,
    ANALYSIS        VARCHAR(2000),
    FK_SUBSYSTEM    INTEGER       not null
        constraint FK_SBSTM
            references MSG_SUBSYSTEM,
    ACTUAL_SQL      VARCHAR(4000) not null,
    CREATE_TMSTAMP  TIMESTAMP(6)  not null,
    STATUS          SMALLINT default 0,
    RUNINPAST       CHAR(1)  default '1',
    RECIPIENT       SMALLINT default 0,
    IMPORTED        TIMESTAMP(6),
    ACTUAL_SQL_CLOB CLOB(1048576)
);

