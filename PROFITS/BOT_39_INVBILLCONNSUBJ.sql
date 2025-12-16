create table BOT_39_INVBILLCONNSUBJ
(
    INVBILLCONNSUBJ_ID         INTEGER generated always as identity
        constraint BOT_39_INVBILLCONNSUBJ_ID_PK
            primary key,
    FK_INVOICEBILL             INTEGER
        constraint BOT_39_FKINVOICEBILL
            references BOT_19_INVOICEBILL,
    KEY                        VARCHAR(32) not null,
    FK_BOT_30_CONNECTEDSUBJECT INTEGER
        constraint FK_BOT_39_BOT_30__
            references BOT_30_CONNECTEDSUBJECT
);

