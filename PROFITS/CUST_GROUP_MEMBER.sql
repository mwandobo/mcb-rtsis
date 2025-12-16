create table CUST_GROUP_MEMBER
(
    FK_CUSTOMERCUST_ID  INTEGER,
    FK_CUST_GROUPGROUP  INTEGER,
    FK_GENERIC_DETASER  INTEGER,
    TMSTAMP             TIMESTAMP(6),
    ENTRY_STATUS        CHAR(1),
    FK_GENERIC_DETAFK   CHAR(5),
    ENTRY_COMMENTS      VARCHAR(30),
    DEP_ACC_NUM_FIRST   DECIMAL(11),
    DEP_ACC_NUM_SECOND  DECIMAL(11),
    ENTRY_PERC          DECIMAL(5, 2),
    PAYMENT_TYPE        CHAR(1) default ' ',
    SWIFT_SHORT_BIC     CHAR(11),
    INSTRUMENT_JUSTIFIC INTEGER,
    PARTIC_ACCOUNT      CHAR(40)
);

comment on column CUST_GROUP_MEMBER.ENTRY_PERC is 'Participation percentage for syndications for corporate loans.';

comment on column CUST_GROUP_MEMBER.PAYMENT_TYPE is 'Outgoing payment method choice:1-PROFITS account2-SWIFT message3-Domestic Interbank message';

comment on column CUST_GROUP_MEMBER.SWIFT_SHORT_BIC is 'Bank BIC for SWIFT/Domestic Interbank';

comment on column CUST_GROUP_MEMBER.INSTRUMENT_JUSTIFIC is 'Domestic Interbank channel code (e.g. SEPA)';

comment on column CUST_GROUP_MEMBER.PARTIC_ACCOUNT is 'Participant''s account (PROFITS, account for SWIFT/Domestic Interbank message)';

create unique index IXU_CUS_008
    on CUST_GROUP_MEMBER (FK_CUSTOMERCUST_ID, FK_CUST_GROUPGROUP);

create unique index SIXCGR01
    on CUST_GROUP_MEMBER (ENTRY_STATUS, FK_CUST_GROUPGROUP);

