create table BOT_75_VOTERREGNUMBER
(
    VOTERREGNUMBER_ID         INTEGER generated always as identity
        constraint BOT_75_VOTERREGNUMBER_ID_PK
            primary key,
    FK_IDENTIFICATIONS        INTEGER
        constraint BOT_75_FKIDENTIFICATIONS
            references BOT_62_IDENTIFICATIONS,
    NUMBEROFVOTERREGISTRATION VARCHAR(16) not null,
    DATEOFEXPIRATION          DATE,
    DATEOFISSUANCE            DATE,
    ISSUANCELOCATION          VARCHAR(32),
    ISSUEDBY                  VARCHAR(128),
    REPORTING_DATE            DATE
);

