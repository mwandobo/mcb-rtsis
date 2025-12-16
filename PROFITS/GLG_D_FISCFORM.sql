create table GLG_D_FISCFORM
(
    FK_GLG_H_FISCFOFOR CHAR(2),
    LINE_NUM           INTEGER,
    COLUMN_NO          SMALLINT,
    FK_CURRENCYID_CURR INTEGER,
    OPERATION_LINE     INTEGER,
    INPUT_AMOUNT       DECIMAL(15, 2),
    DEACTIVATION_DATE  DATE,
    TIMESTMP           TIMESTAMP(6),
    STATUS             CHAR(1),
    PRINT_FLAG         CHAR(1),
    OPERATION          CHAR(1),
    LINE_TYPE          CHAR(1),
    CONVERT0           CHAR(1),
    DB_CR_BAL_IND      CHAR(1),
    FK_GLG_ACCOUNTACCO CHAR(21),
    DESCR              CHAR(50)
);

create unique index IXU_GLG_060
    on GLG_D_FISCFORM (FK_GLG_H_FISCFOFOR, LINE_NUM);

