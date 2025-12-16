create table INTEREST
(
    ID_INTEREST        INTEGER not null
        constraint PKINTERE
            primary key,
    DAYS_RETRIEVAL_IND CHAR(1),
    DAYSBASE           SMALLINT,
    AVG_VAL_BALNC      CHAR(1),
    APPLY_INTER        CHAR(1),
    USAGE_COUNTER      SMALLINT,
    NEGOTIATION_FLAG   CHAR(1),
    FOR_DEBIT          CHAR(1),
    ENTRY_STATUS       CHAR(1),
    TMSTAMP            TIMESTAMP(6),
    DESCRIPTION        VARCHAR(40)
);

