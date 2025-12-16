create table IPS_MESSAGE_HEADER_COLL_EXTEND
(
    ORDER_CODE                 VARCHAR(20) not null
        constraint IX_IPS_MSG_HDR_COLL_EXTND
            primary key,
    FK_MANDATE_SN              INTEGER     not null,
    MANDATE_ID                 VARCHAR(35),
    CREDITOR_CODE              VARCHAR(35),
    DEBTOR_IBAN_ACCOUNT        VARCHAR(37),
    PRODUCT_TYPE               VARCHAR(10),
    SEQUENCE_TYPE              CHAR(4),
    DUE_DATE                   DATE,
    CREDITOR_IBAN_ACCOUNT      VARCHAR(37),
    LATEST_RETURN_DATE         DATE,
    LATEST_REFUND_DATE         DATE,
    LATEST_UNAUTH_REFUND_DATE  DATE,
    LATEST_REVERSE_DATE        DATE,
    LATEST_RETURN_REVERSE_DATE DATE,
    INSTRUCTION_ID             VARCHAR(35)
);

comment on table IPS_MESSAGE_HEADER_COLL_EXTEND is 'Extra information about SDD Collections - Pacs003';

comment on column IPS_MESSAGE_HEADER_COLL_EXTEND.ORDER_CODE is 'Unique field (Primary Key) for each message. Generated from a counter.';

comment on column IPS_MESSAGE_HEADER_COLL_EXTEND.FK_MANDATE_SN is 'Auto Identity Column of SDD_MANDATES';

comment on column IPS_MESSAGE_HEADER_COLL_EXTEND.MANDATE_ID is 'The Unique Mandate Reference Number';

comment on column IPS_MESSAGE_HEADER_COLL_EXTEND.CREDITOR_CODE is 'This contains the Creditor Scheme identifier (CID)';

comment on column IPS_MESSAGE_HEADER_COLL_EXTEND.DEBTOR_IBAN_ACCOUNT is 'International Bank Account Number (IBAN) of the  Debtor';

comment on column IPS_MESSAGE_HEADER_COLL_EXTEND.PRODUCT_TYPE is 'The SDD product-CORE-B2B-B2BU-B2CU  etc';

comment on column IPS_MESSAGE_HEADER_COLL_EXTEND.SEQUENCE_TYPE is 'Sequense type: FRST OOFF, RCUR, FNAL';

comment on column IPS_MESSAGE_HEADER_COLL_EXTEND.DUE_DATE is 'Requested Collection Date';

comment on column IPS_MESSAGE_HEADER_COLL_EXTEND.CREDITOR_IBAN_ACCOUNT is 'Creditor IBAN Account';

comment on column IPS_MESSAGE_HEADER_COLL_EXTEND.LATEST_RETURN_DATE is 'Last Allowed Return Date';

comment on column IPS_MESSAGE_HEADER_COLL_EXTEND.LATEST_REFUND_DATE is 'Last Allowed Refund Date';

comment on column IPS_MESSAGE_HEADER_COLL_EXTEND.LATEST_UNAUTH_REFUND_DATE is 'Latest Unauthorized Refund Date';

comment on column IPS_MESSAGE_HEADER_COLL_EXTEND.LATEST_REVERSE_DATE is 'Latest Allowed Reversal Date';

comment on column IPS_MESSAGE_HEADER_COLL_EXTEND.LATEST_RETURN_REVERSE_DATE is 'Latest Allowed Return Of Reversal Date';

create unique index IX_FK_MANDATE_SN
    on IPS_MESSAGE_HEADER_COLL_EXTEND (FK_MANDATE_SN);

create unique index IX_INSTRUCTION_ID
    on IPS_MESSAGE_HEADER_COLL_EXTEND (INSTRUCTION_ID);

create unique index IX_MANDATE_ID
    on IPS_MESSAGE_HEADER_COLL_EXTEND (MANDATE_ID);

