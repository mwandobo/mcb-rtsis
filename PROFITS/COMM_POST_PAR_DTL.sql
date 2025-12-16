create table COMM_POST_PAR_DTL
(
    FK_COMM_POST_PASERIAL_NUM INTEGER,
    SERIAL_NUM                DECIMAL(10),
    DCD_PRFT_SYS              SMALLINT,
    CURRENT_CYCLE             SMALLINT,
    DEBITS_POSTED             SMALLINT,
    DEBITS_PER_CHEQUE         SMALLINT,
    DEBIT_ON_CYCLE            SMALLINT,
    CYCLES_TO_DEBIT           SMALLINT,
    PAR_RULE_ID               INTEGER,
    ID_JUSTIFIC               INTEGER,
    BILL_SERIAL_NUM           DECIMAL(10),
    DCD_RULE_ID               DECIMAL(12),
    ACC_NUMBER                DECIMAL(15),
    CHEQUE_ISSUE_DATE         DATE,
    CYCLE_DATE                DATE,
    ENTRY_STATUS              CHAR(1),
    TYPE                      CHAR(2),
    CHEQUE_NUMBER             CHAR(20),
    DESCR                     CHAR(200),
    CYCLES                    SMALLINT,
    CYCLES_PERFORMED          SMALLINT,
    FK_COMMPOSTPARSN_HDR      INTEGER
);

create unique index IXU_COM_000
    on COMM_POST_PAR_DTL (FK_COMM_POST_PASERIAL_NUM, SERIAL_NUM);

