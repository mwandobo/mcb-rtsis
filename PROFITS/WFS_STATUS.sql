create table WFS_STATUS
(
    WF_STATUS_ID       DECIMAL(10) not null
        constraint PK_WF_STATUS
            primary key,
    DESCRIPTION        VARCHAR(50),
    DESCRIPTION_DETAIL VARCHAR(2048),
    DESCRIPTION_SHORT  VARCHAR(10),
    ABBREVIATION       VARCHAR(10),
    FLOW_STATUS        CHAR(1),
    CREATE_UNIT        INTEGER,
    CREATE_DATE        DATE,
    CREATE_USR         CHAR(8),
    CREATE_TMSTAMP     TIMESTAMP(6),
    UPDATE_UNIT        INTEGER,
    UPDATE_DATE        DATE,
    UPDATE_USR         CHAR(8),
    UPDATE_TMSTAMP     TIMESTAMP(6),
    WF_STATUS_STS      CHAR(1)
);

