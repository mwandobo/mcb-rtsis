create table SDD_MANDATES
(
    MANDATE_SN              INTEGER     not null,
    MANDATE_ID              VARCHAR(35) not null,
    CREDITOR_CODE           VARCHAR(35) not null,
    DEBTOR_IBAN_ACCOUNT     VARCHAR(37) not null,
    ACCOUNT_NUMBER          CHAR(40),
    CUST_ID                 INTEGER,
    PRODUCT_TYPE            VARCHAR(10),
    SEQUENCE_TYPE           CHAR(4),
    FLOW_TYPE               CHAR(3),
    ENTRY_TYPE              CHAR(5),
    STATUS                  CHAR(1),
    INACT_REASON_FLG        CHAR(1),
    INACT_REASON_CODE       CHAR(5),
    DATE_OF_SIGNATURE       DATE,
    LAST_TRX_DATE           DATE,
    RECALL_DATE             DATE,
    MAX_AMOUNT              DECIMAL(15, 2),
    DEBTOR_NAME             VARCHAR(70),
    DEBTOR_ADDRESS_1        VARCHAR(70),
    DEBTOR_ADDRESS_2        VARCHAR(70),
    DEBTOR_COUNTRY          CHAR(2),
    DEBTOR_ID_TYPE          VARCHAR(10),
    DEBTOR_ID_NUMBER        VARCHAR(35),
    DEBTOR_BIC              VARCHAR(11),
    DEBTOR_BRANCH_ID        VARCHAR(35),
    ULT_DEBTOR_NAME         VARCHAR(70),
    ULT_DEBTOR_ADDRESS_1    VARCHAR(70),
    ULT_DEBTOR_ADDRESS_2    VARCHAR(70),
    ULT_DEBTOR_COUNTRY      CHAR(2),
    ULT_DEBTOR_ID_TYPE      VARCHAR(10),
    ULT_DDEBTOR_ID_NUMBER   VARCHAR(35),
    SUSPENSION_START_DATE   DATE,
    SUSPENSION_END_DATE     DATE,
    CANCEL_NEXT_PAYMENT_FLG CHAR(1),
    constraint IX_SDDMND
        primary key (DEBTOR_IBAN_ACCOUNT, CREDITOR_CODE, MANDATE_ID, MANDATE_SN)
);

comment on column SDD_MANDATES.MANDATE_SN is 'Auto Identity Column of SDD_MANDATES';

comment on column SDD_MANDATES.MANDATE_ID is 'The Unique Mandate Reference Number';

comment on column SDD_MANDATES.CREDITOR_CODE is 'This contains the Creditor Scheme identifier (CID)';

comment on column SDD_MANDATES.DEBTOR_IBAN_ACCOUNT is 'International Bank Account Number (IBAN) of the  Debtor';

comment on column SDD_MANDATES.ACCOUNT_NUMBER is 'Profits Account Number';

comment on column SDD_MANDATES.CUST_ID is 'Profits Customer Id';

comment on column SDD_MANDATES.PRODUCT_TYPE is 'The SDD product-CORE-B2B-B2BU-B2CU  etc';

comment on column SDD_MANDATES.SEQUENCE_TYPE is 'Identifies the direct debit sequence, such as first, recurrent, final or one-off';

comment on column SDD_MANDATES.FLOW_TYPE is 'Possible Values: DMF,CMF (Debtor Mandate Flow/ Creditor Mandate Flow)';

comment on column SDD_MANDATES.ENTRY_TYPE is 'Mandate Entry Type.  Possible Values:PAPER / eMNDT';

comment on column SDD_MANDATES.STATUS is 'Mandate Status: 0:  Inactive   1: Active';

comment on column SDD_MANDATES.INACT_REASON_FLG is 'Mandate''s Inactivation Reason1.     36 Months without transaction2.     Recalled  by Client 3.     Recalled by Bank4.     Recalled by collection (Last of  recurring or First of OOFF)';

comment on column SDD_MANDATES.INACT_REASON_CODE is 'Possible Values:AC01: Incorrect Acnt NbrAC04: Closed AcntAG01: Transaction not allowed on this type of acntMD01: Mandate is cancelled or InvalidMD07: End Customer Deceased';

comment on column SDD_MANDATES.DATE_OF_SIGNATURE is 'Date on which the direct debit mandate has been signed by the debtor.';

comment on column SDD_MANDATES.LAST_TRX_DATE is 'Last Collection Date - Used to validate 36 months of no activity';

comment on column SDD_MANDATES.RECALL_DATE is 'Recall Date of the Mandate';

comment on column SDD_MANDATES.MAX_AMOUNT is 'Mandate''s maximum allowed amount to debit';

comment on column SDD_MANDATES.DEBTOR_NAME is 'Debtor name';

comment on column SDD_MANDATES.DEBTOR_ADDRESS_1 is 'Debtor Address Line 1';

comment on column SDD_MANDATES.DEBTOR_ADDRESS_2 is 'Debtor Address Line 2';

comment on column SDD_MANDATES.DEBTOR_COUNTRY is 'Debtor Country';

comment on column SDD_MANDATES.DEBTOR_ID_TYPE is 'Debtor Identification TypePossible Values: OrgId  PrvtId';

comment on column SDD_MANDATES.DEBTOR_ID_NUMBER is 'Debtor Identification Number';

comment on column SDD_MANDATES.DEBTOR_BIC is 'Debtor BIC';

comment on column SDD_MANDATES.DEBTOR_BRANCH_ID is 'Debtor Branch Identification';

comment on column SDD_MANDATES.ULT_DEBTOR_NAME is 'Ultimate Debtor Name';

comment on column SDD_MANDATES.ULT_DEBTOR_ADDRESS_1 is 'Ultimate Dbtr Address Line1';

comment on column SDD_MANDATES.ULT_DEBTOR_ADDRESS_2 is 'Ultimate Dbtr Address Line2';

comment on column SDD_MANDATES.ULT_DEBTOR_COUNTRY is 'Ultimate Debtor Country';

comment on column SDD_MANDATES.ULT_DEBTOR_ID_TYPE is 'Ultimate Debtor Identification TypePossible Values: OrgId  PrvtId';

comment on column SDD_MANDATES.ULT_DDEBTOR_ID_NUMBER is 'ULTIMATE DEBTOR IDENTIFICATION NUMBER';

comment on column SDD_MANDATES.SUSPENSION_START_DATE is 'Mandates payment suspension start date';

comment on column SDD_MANDATES.SUSPENSION_END_DATE is 'Mandates payment suspension end date';

comment on column SDD_MANDATES.CANCEL_NEXT_PAYMENT_FLG is 'Possible Values: 0:  Inactive   1: Active';

