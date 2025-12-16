create table WFS_ACTION
(
    WF_ACTION_ID       DECIMAL(10) not null
        constraint PK_WF_ACTION
            primary key,
    DESCRIPTION        VARCHAR(50),
    DESCRIPTION_DETAIL VARCHAR(2048),
    DESCRIPTION_SHORT  VARCHAR(10),
    ABBREVIATION       VARCHAR(10),
    CREATE_UNIT        INTEGER,
    CREATE_DATE        DATE,
    CREATE_USR         CHAR(8),
    CREATE_TMSTAMP     TIMESTAMP(6),
    UPDATE_UNIT        INTEGER,
    UPDATE_DATE        DATE,
    UPDATE_USR         CHAR(8),
    UPDATE_TMSTAMP     TIMESTAMP(6),
    WF_ACTION_STS      CHAR(1)
);

