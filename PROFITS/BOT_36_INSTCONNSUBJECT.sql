create table BOT_36_INSTCONNSUBJECT
(
    INSTCONNSUBJECT_ID         INTEGER generated always as identity
        constraint BOT_36_INSTCONNSUBJECT_ID_PK
            primary key,
    FK_INSTALMENT              INTEGER
        constraint BOT_36_FKINSTALMENT
            references BOT_17_INSTALMENT,
    KEY                        VARCHAR(32) not null,
    FK_BOT_30_CONNECTEDSUBJECT INTEGER
        constraint FK_BOT_36_BOT_30__
            references BOT_30_CONNECTEDSUBJECT,
    CUST_ID                    VARCHAR(32),
    LOAN_CODE                  VARCHAR(40),
    REPORTING_DATE             DATE
);

