create table SDD_MANDATE_AMENDMENT_HISTORY
(
    FK_MANDATE_SN            INTEGER      not null,
    MANDATE_ID               VARCHAR(35)  not null,
    CREDITOR_CODE            VARCHAR(35)  not null,
    DEBTOR_IBAN_ACCOUNT      CHAR(37)     not null,
    AMENDMENT_TIMESTAMP      TIMESTAMP(6) not null,
    MODIFIED_FIELD_INDICATOR CHAR(10),
    NEW_CREDITOR_CODE        VARCHAR(35),
    NEW_MANDATE_ID           VARCHAR(35),
    NEW_CREDITOR_NAME        VARCHAR(70),
    NEW_DEBTOR_IBAN          CHAR(37),
    NEW_MAX_AMOUNT           DECIMAL(15, 2),
    NEW_MANDATE_STATUS       CHAR(1),
    TRX_UNIT                 INTEGER,
    TRX_DATE                 DATE,
    TRX_USER                 CHAR(8),
    TRX_USR_SN               INTEGER,
    constraint IX_SDD_MND_AMND_HI
        primary key (AMENDMENT_TIMESTAMP, DEBTOR_IBAN_ACCOUNT, CREDITOR_CODE, MANDATE_ID, FK_MANDATE_SN)
);

comment on table SDD_MANDATE_AMENDMENT_HISTORY is 'Mandate Amendment History 1.   .2.   3.       4.    .5.   Mandate';

comment on column SDD_MANDATE_AMENDMENT_HISTORY.FK_MANDATE_SN is 'Auto Identity Column of SDD_MANDATES';

comment on column SDD_MANDATE_AMENDMENT_HISTORY.MANDATE_ID is 'The Unique Mandate Reference Number';

comment on column SDD_MANDATE_AMENDMENT_HISTORY.CREDITOR_CODE is 'This contains the Creditor Scheme identifier (CID)';

comment on column SDD_MANDATE_AMENDMENT_HISTORY.DEBTOR_IBAN_ACCOUNT is 'International Bank Account Number (IBAN) of the  Debtor';

comment on column SDD_MANDATE_AMENDMENT_HISTORY.AMENDMENT_TIMESTAMP is 'Mandate Amendment Timestamp';

comment on column SDD_MANDATE_AMENDMENT_HISTORY.MODIFIED_FIELD_INDICATOR is 'Modified Field Indicator     .        byte    1.   Plafond      byte    1.';

