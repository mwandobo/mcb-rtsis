create table SCH_GROUP_SSN
(
    SESSION_ID          TIMESTAMP(6)         not null,
    FK_SCRIPT           VARCHAR(40)          not null,
    ID                  VARCHAR(40)          not null,
    NAME                VARCHAR(100)         not null,
    ENABLED             SMALLINT   default 0 not null,
    EMAIL               VARCHAR(100),
    PARENT              VARCHAR(40),
    POSITION            SMALLINT   default 0 not null,
    BREAK               SMALLINT   default 0,
    MULTITHREADED       SMALLINT   default 0,
    FIREANDFORGET       SMALLINT   default 0,
    SECONDRUNPARAMETERS DECIMAL(1) default 0,
    FILELOOP            VARCHAR(250),
    ACTIVATION_QUERY    CLOB(1048576)
);

