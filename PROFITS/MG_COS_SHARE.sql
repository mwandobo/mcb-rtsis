create table MG_COS_SHARE
(
    FILE_NAME          CHAR(50)    not null,
    SERIAL_NO          DECIMAL(10) not null,
    ROW_STATUS         SMALLINT,
    SHARE_STATUS       SMALLINT,
    MIG_BLOCKED        INTEGER,
    MEMBER_ID          DECIMAL(10),
    SHARE_ID           DECIMAL(10),
    MIG_PRV_OWNER      DECIMAL(10),
    NOMINAL_PRICE      DECIMAL(15, 2),
    UTF_NUM1           DECIMAL(15, 2),
    ACCOUNTING_PRICE   DECIMAL(15, 2),
    ASSESSMENT_PRICE   DECIMAL(15, 2),
    PURCHASE_PRICE     DECIMAL(15, 2),
    UTF_NUM2           DECIMAL(15, 2),
    UTF_DATE2          DATE,
    UTF_DATE1          DATE,
    BLOCKING_EXPIRE_DT DATE,
    CREATION_DATE      DATE,
    DELETION_DATE      DATE,
    ROW_PROCESS_DATE   DATE,
    LAST_ACQUITS_DATE  DATE,
    ROW_TMSTAMP        TIMESTAMP(6),
    UTF_TEXT2          CHAR(80),
    ROW_ERR_DESC       CHAR(80),
    UTF_TEXT1          CHAR(80),
    constraint IXU_MIG_017
        primary key (FILE_NAME, SERIAL_NO)
);

create unique index BKEYSHAR
    on MG_COS_SHARE (SHARE_ID);

create unique index IDXRUN1
    on MG_COS_SHARE (MEMBER_ID);

