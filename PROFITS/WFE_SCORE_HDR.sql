create table WFE_SCORE_HDR
(
    CUST_ID                    INTEGER     not null,
    FK_WFS_SCORECARD           DECIMAL(10) not null,
    SCORE_DATE                 DATE        not null,
    SCORE_HDR_SN               DECIMAL(10) not null,
    APPLICATION_SYSTEM         SMALLINT,
    APPLICATION_ID             CHAR(40),
    SCORING_RESULT             DECIMAL(5, 2),
    SCORING_COMMENTS           VARCHAR(500),
    SCORE_FROM                 DECIMAL(5, 2),
    SCORE_TO                   DECIMAL(5, 2),
    SCORE_DESCRIPTION          VARCHAR(80),
    CREATE_UNIT                INTEGER,
    CREATE_DATE                DATE,
    CREATE_USR                 CHAR(8),
    CREATE_TMSTAMP             TIMESTAMP(6),
    UPDATE_UNIT                INTEGER,
    UPDATE_DATE                DATE,
    UPDATE_USR                 CHAR(8),
    UPDATE_TMSTAMP             TIMESTAMP(6),
    ENTRY_STATUS               CHAR(1),
    SCORING_ORIGINAL_RESULT    DECIMAL(5, 2),
    SCORE_ORIGINAL_FROM        DECIMAL(5, 2),
    SCORE_ORIGINAL_TO          DECIMAL(5, 2),
    SCORE_ORIGINAL_DESCRIPTION VARCHAR(80),
    constraint PK_WFE_SCORE_HDR
        primary key (CUST_ID, FK_WFS_SCORECARD, SCORE_HDR_SN, SCORE_DATE)
);

