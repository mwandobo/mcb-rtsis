create table BOT_30_CONNECTEDSUBJECT
(
    CONNECTEDSUBJECT_ID    INTEGER generated always as identity
        constraint BOT_30_CONNECTEDSUBJECT_ID_PK
            primary key,
    COLLATERALTYPE         INTEGER,
    COLLATERALVALUE        DECIMAL(19, 4),
    COMMENT1               VARCHAR(128),
    ROLEOFCLIENT           INTEGER not null,
    FK_BOT_2_SUBJECTCHOICE INTEGER
        constraint FK_BOT_30_BOT_2__
            references BOT_2_SUBJECTCHOICE,
    LOAN_CODE              VARCHAR(40),
    REPORTING_DATE         DATE,
    CUST_ID                VARCHAR(32)
);

