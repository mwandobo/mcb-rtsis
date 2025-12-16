create table GLG_H_YEAR_RANGE
(
    TRAN_ID            CHAR(6),
    SEQUENCE0          SMALLINT,
    TIMESTMP           TIMESTAMP(6),
    DEACTIVATION_DATE  DATE,
    STATUS             CHAR(1),
    PHASE              CHAR(1),
    CHANGE_STATUS_FLAG CHAR(1),
    FK_GLG_DOCUMENTDO0 CHAR(2),
    FK_GLG_DOCUMENTDOC CHAR(4),
    REMARKS            CHAR(20),
    FK_GLG_ACCOUNTACCO CHAR(21),
    DESCR              VARCHAR(30)
);

create unique index IXU_GLG_078
    on GLG_H_YEAR_RANGE (TRAN_ID);

