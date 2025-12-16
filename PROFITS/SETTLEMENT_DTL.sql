create table SETTLEMENT_DTL
(
    FK_HDR_FILE_ID           DECIMAL(11) not null,
    DETAIL_SEQ_NUM           DECIMAL(7)  not null,
    RECORD_TYPE              VARCHAR(2),
    SETTL_BANK               CHAR(2),
    NUMBER_NORMAL_OTHER_BANK DECIMAL(6),
    VALUE_NORMAL_OTHER_BANK  DECIMAL(15, 2),
    NUMBER_UNPAID_OTHER_BANK DECIMAL(6),
    VALUE_UNPAID_OTHER_BANK  DECIMAL(15, 2),
    LINE_TEXT                VARCHAR(78),
    SESSION_NO               CHAR(1),
    constraint PK_SETTLEMENT_DTL
        primary key (FK_HDR_FILE_ID, DETAIL_SEQ_NUM)
);

comment on column SETTLEMENT_DTL.FK_HDR_FILE_ID is 'Associated Id of the file header (SETTLEMENT_HDR table)';

comment on column SETTLEMENT_DTL.DETAIL_SEQ_NUM is 'Identity auto number';

comment on column SETTLEMENT_DTL.RECORD_TYPE is '06 for Cheques, 07 EFT Debits, 08 for EFT Credits, 09 for Manual cheques';

comment on column SETTLEMENT_DTL.SETTL_BANK is 'The Settlement Other Bank associated to the COLLABORATION_BANK.ACH_BANK_CODE field';

comment on column SETTLEMENT_DTL.LINE_TEXT is 'The full length Text of the file line.';

