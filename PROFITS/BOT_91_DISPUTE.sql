create table BOT_91_DISPUTE
(
    DISPUTE_ID             INTEGER generated always as identity
        constraint BOT_91_DISPUTE_ID_PK
            primary key,
    DATEOFDISPUTERESOLVING DATE,
    REASONOFTHEDISPUTE     INTEGER,
    UNRESOLVEDDISPUTE      INTEGER not null
);

