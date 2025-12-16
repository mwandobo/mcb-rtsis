create table CLC_AGENCY_ASSIGN
(
    CASE_ID            CHAR(40)    not null,
    AGENCY_ID          DECIMAL(10) not null,
    AGENT_ID           DECIMAL(10) not null,
    ASSIGN_DESCRIPTION VARCHAR(80),
    ASSIGN_DETAILS     VARCHAR(4000),
    DATE_ASSIGNED      DATE,
    DATE_ACKNOWLEDGE   DATE,
    DATE_REVIEWED      DATE,
    DATE_CLOSED        DATE,
    CREATE_UNIT        INTEGER,
    CREATE_DATE        DATE,
    CREATE_USER        CHAR(8),
    CREATE_TMSTAMP     TIMESTAMP(6),
    UPDATE_UNIT        INTEGER,
    UPDATE_DATE        DATE,
    UPDATE_USER        CHAR(8),
    UPDATE_TMSTAMP     TIMESTAMP(6),
    ASSIGN_STATUS      CHAR(2),
    constraint CLC_COLLECT_PK_7
        primary key (CASE_ID, AGENCY_ID, AGENT_ID)
);

