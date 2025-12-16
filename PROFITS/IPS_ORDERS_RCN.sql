create table IPS_ORDERS_RCN
(
    ID                           DECIMAL(15)  not null
        constraint PKEY_ORDERS_RCN
            primary key,
    ORDER_CODE                   VARCHAR(20)  not null,
    ORDER_REFERENCE              VARCHAR(40)  not null,
    DIRECTION                    VARCHAR(3)   not null,
    TRX_UNIT                     INTEGER,
    TRX_DATE                     DATE,
    ORDER_STATUS                 CHAR(1),
    ORDER_MESSAGE_TYPE           VARCHAR(35),
    ORDER_SETTL_AMOUNT           DECIMAL(15, 2),
    INTERBANK_SETTLE_DT          DATE,
    ACCP_DATETIME                VARCHAR(40),
    RCN_FILE_NAME                VARCHAR(250),
    RCN_FILE_DATE                DATE,
    RCN_MSG_REFERENCE            VARCHAR(40)  not null,
    RCN_TX_REFERENCE             VARCHAR(40)  not null,
    RCN_TX_ORIG_REFERENCE        VARCHAR(40),
    RCN_TX_ORIG_SETTL_AMOUNT     DECIMAL(15, 2),
    RCN_TX_STATUS                CHAR(4),
    RCN_TX_REASON_CODE           VARCHAR(35),
    RCN_TX_ORIG_MESSAGE_TYPE     VARCHAR(35),
    RCN_ACCP_DATETIME            VARCHAR(40),
    RCN_ORIG_SETTLE_DATE         DATE,
    STATUS_RECONCILED            VARCHAR(1),
    STATUS_ACTION_PROCESSED      VARCHAR(1),
    RCN_ACTION                   VARCHAR(40),
    RCN_RESULT                   VARCHAR(70),
    RCN_EXEC_TMSTAMP             TIMESTAMP(6),
    RCN_TRX_UNIT                 INTEGER,
    RCN_TRX_DATE                 DATE,
    RCN_TRX_USER                 CHAR(8),
    RCN_TRX_USR_SN               INTEGER,
    CREATION_TIMESTAMP           TIMESTAMP(6) not null,
    RCN_TX_ORIG_DEBTOR_ACCOUNT   VARCHAR(40),
    RCN_TX_ORIG_CREDITOR_ACCOUNT VARCHAR(40),
    RCN_TX_ORIG_DEBTOR_BIC       VARCHAR(12),
    RCN_TX_ORIG_CREDITOR_BIC     VARCHAR(12),
    RCN_ORIGINATOR_BIC           VARCHAR(12),
    ENTRY_COMMENTS               VARCHAR(2048)
);

comment on table IPS_ORDERS_RCN is 'Reconciliation table for sct inst and online payments';

comment on column IPS_ORDERS_RCN.ID is 'Primary key as a sequence';

comment on column IPS_ORDERS_RCN.ORDER_CODE is 'IPS_MESSAGE_HEADER.ORDER_CODE';

comment on column IPS_ORDERS_RCN.ORDER_REFERENCE is 'OUT: IPS_MESSAGE_HEADER.ORDER_CODEIN:  IPS_MESSAGE_HEADER.EXTERNAL_ID_REF';

comment on column IPS_ORDERS_RCN.DIRECTION is 'IPS_MESSAGE_HEADER.ORDER_TYPE (IN/OUT)';

comment on column IPS_ORDERS_RCN.TRX_UNIT is 'IPS_MESSAGE_HEADER.TRX_UNIT';

comment on column IPS_ORDERS_RCN.ORDER_STATUS is 'IPS_MESSAGE_HEADER.ORDER_STATUS';

comment on column IPS_ORDERS_RCN.ORDER_MESSAGE_TYPE is 'Derieve from IPS_MESSAGE_HEADER (e.g pacs.008, pacs.004 etc)';

comment on column IPS_ORDERS_RCN.INTERBANK_SETTLE_DT is 'IPS_MESSAGE_HEADER.INTERBANK_SETTLE_DATE';

comment on column IPS_ORDERS_RCN.RCN_FILE_NAME is 'XML_INPUT_FILE.FILE_NAME';

comment on column IPS_ORDERS_RCN.RCN_FILE_DATE is 'Date of the Reconcile file as extracted frm its filename';

comment on column IPS_ORDERS_RCN.RCN_MSG_REFERENCE is 'ORE.INST Pacs002.GrpHdr.MsgId';

comment on column IPS_ORDERS_RCN.RCN_TX_REFERENCE is 'ORE.INST Pacs002.TxInfAndSts.StsId';

comment on column IPS_ORDERS_RCN.RCN_TX_ORIG_REFERENCE is 'ORE.INST Pacs002.TxInfAndSts.OrgnlTxId';

comment on column IPS_ORDERS_RCN.RCN_TX_ORIG_SETTL_AMOUNT is 'ORE.INST Pacs002.TxInfAndSts.OrgnlTxRef.IntrBkSttlmAmt';

comment on column IPS_ORDERS_RCN.RCN_TX_STATUS is 'ORE.INST Pacs002.TxInfAndSts.TxSts ACCP/RJCT';

comment on column IPS_ORDERS_RCN.RCN_TX_REASON_CODE is 'ORE.INST Pacs002.TxInfAndSts.StsRsnInf.RsnCd orORE.INST Pacs002.TxInfAndSts.StsRsnInf.RsnPrtry';

comment on column IPS_ORDERS_RCN.RCN_TX_ORIG_MESSAGE_TYPE is 'ORE.INST Pacs002.OrgnlGrpInfAndSts.OrgnlMsgNmId';

comment on column IPS_ORDERS_RCN.RCN_ACCP_DATETIME is 'ORE.INST Pacs002.TxInfAndSts.AccptncDtTm';

comment on column IPS_ORDERS_RCN.RCN_ORIG_SETTLE_DATE is 'ORE.INST Pacs002.TxInfAndSts.OrgnlTxRef.IntrBkSttlmDt';

comment on column IPS_ORDERS_RCN.STATUS_RECONCILED is '      CBS &  ORE.INST        Reference   Status  CBS  ORE.INST0: Reconciliation Pending  -           1: Matched2:';

comment on column IPS_ORDERS_RCN.STATUS_ACTION_PROCESSED is '0: Action Pending1: Action Performed2: No Action Needed3. Investigation Needed';

comment on column IPS_ORDERS_RCN.RCN_ACTION is 'e.g. NOACTION, REVERSE_ORIG_ORDER, CANCEL_ORIG_ORDER etc';

comment on column IPS_ORDERS_RCN.RCN_RESULT is 'The result of the reconciliation processe.g. ACK_IN_CBS_ACCEPTED_BYDIAS, ACK_IN_CBS_REJECT_BYDIAS etc';

comment on column IPS_ORDERS_RCN.RCN_EXEC_TMSTAMP is 'Reconciliation Process Execution Timestamp';

comment on column IPS_ORDERS_RCN.RCN_TRX_UNIT is 'MNT_RECORDING After User Action (Batch or Online)';

comment on column IPS_ORDERS_RCN.RCN_TRX_DATE is 'MNT_RECORDING After User Action (Batch or Online)';

comment on column IPS_ORDERS_RCN.RCN_TRX_USER is 'MNT_RECORDING After User Action (Batch or Online)';

comment on column IPS_ORDERS_RCN.RCN_TRX_USR_SN is 'MNT_RECORDING After User Action (Batch or Online)';

comment on column IPS_ORDERS_RCN.CREATION_TIMESTAMP is 'Record Creation Timestamp';

comment on column IPS_ORDERS_RCN.RCN_TX_ORIG_DEBTOR_ACCOUNT is 'ORE.INST Pacs002.TxInfAndSts.OrgnlTxRef.DbtrAcct.Id.IBAN';

comment on column IPS_ORDERS_RCN.RCN_TX_ORIG_CREDITOR_ACCOUNT is 'ORE.INST Pacs002.TxInfAndSts.OrgnlTxRef.CdtrAcct.Id.IBAN';

comment on column IPS_ORDERS_RCN.RCN_TX_ORIG_DEBTOR_BIC is '/DSCTfh:FIToFIPmtStsRpt/sw2:TxInfAndSts/sw2:OrgnlTxRef/sw2:DbtrAgt/sw2:FinInstnId/sw2:BIC';

comment on column IPS_ORDERS_RCN.RCN_TX_ORIG_CREDITOR_BIC is '/DSCTfh:FIToFIPmtStsRpt/sw2:TxInfAndSts/sw2:OrgnlTxRef/sw2:CdtrAgt/sw2:FinInstnId/sw2:BIC';

