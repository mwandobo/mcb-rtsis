create table SWIFT_541
(
    TRX_REF_NO         CHAR(16),
    PRIORITY_CODE      CHAR(2),
    SENDER_BIC         CHAR(12),
    RECEIVER_BIC       CHAR(12),
    ORDERING_INSTI     CHAR(183),
    BENEF_INSTIT       CHAR(183),
    CREATION_DATE      CHAR(12),
    SETTLEMENT_DATE    CHAR(12),
    PRICE              CHAR(26),
    ALLOCATION_AMOUNT  CHAR(26),
    ISIN               CHAR(183),
    DEAL_NO            CHAR(26),
    IN_TRX_USR         CHAR(8),
    MSG_CONFIRM_DATE   DATE,
    MSG_CONFIRM_BNKUSR CHAR(8),
    MSG_SEND_TIME      TIMESTAMP(6),
    MSG_SEND_BNKUSR    CHAR(8),
    TMSTAMP            TIMESTAMP(6) not null,
    FK_SWMSG_PRFTREFNO CHAR(16)     not null
        constraint PK_SWIFT_541
            primary key,
    FK_ISSUE_UNIT      INTEGER,
    FK_SWIFT_COUNTEDAT DATE,
    ENTRY_STATUS       CHAR(1)      not null,
    ACCR_AT_BUY_AMNT   CHAR(26),
    WITHOLD_AMOUNT     CHAR(26),
    SETTLEMENT_AMOUNT  CHAR(26)
);

comment on column SWIFT_541.SENDER_BIC is 'It is the SWIFT Address the SWIFT is sent to.';

comment on column SWIFT_541.PRICE is 'It is the Sender''s unambigious identification of the transaction. Its detailed form and content are at the discretion of the Sender.';

comment on column SWIFT_541.ALLOCATION_AMOUNT is 'Field 21: Contains identification of the message to which the current message is related.';

create unique index I0010503
    on SWIFT_541 (FK_ISSUE_UNIT);

create unique index I0010507
    on SWIFT_541 (FK_SWIFT_COUNTEDAT);

