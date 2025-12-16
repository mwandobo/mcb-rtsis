create table BOT_19_INVOICEBILL
(
    INVOICEBILL_ID             INTEGER generated always as identity
        constraint BOT_19_INVOICEBILL_ID_PK
            primary key,
    FK_STORINVOICEBILL         INTEGER
        constraint BOT_19_FKSTORINVOICEBILL
            references BOT_5_STORINVOICEBILL,
    ADDITIONALINFORMATION      VARCHAR(256),
    SUBJECTOFTHEINVOICE        VARCHAR(256),
    TYPEOFINVOICEBILL          INTEGER            not null,
    X__COLLATERAL              SMALLINT default 1,
    X__CONNECTEDSUBJECT        SMALLINT default 1 not null,
    CURRENCYOFLOAN             INTEGER            not null,
    ECONOMICSECTOR             INTEGER            not null,
    NEGATIVESTATUSOFLOAN       INTEGER            not null,
    PASTDUEAMOUNT              DECIMAL(19, 4)     not null,
    PASTDUEDAYS                INTEGER            not null,
    PHASEOFLOAN                INTEGER            not null,
    PURPOSEOFLOAN              INTEGER,
    RESCHEDULEDLOAN            INTEGER            not null,
    TOTALLOANAMOUNT            DECIMAL(19, 4)     not null,
    FK_BOT_72_CONTRACTDATES    INTEGER
        constraint FK_BOT_19_BOT_72__
            references BOT_72_CONTRACTDATES,
    FK_BOT_91_DISPUTE          INTEGER
        constraint FK_BOT_19_BOT_91__
            references BOT_91_DISPUTE,
    FK_BOT_70_FEESANDPENALTIES INTEGER
        constraint FK_BOT_19_BOT_70__
            references BOT_70_FEESANDPENALTIES
);

