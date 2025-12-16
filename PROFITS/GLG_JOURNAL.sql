create table GLG_JOURNAL
(
    JOURNAL_ID         CHAR(2),
    TIMESTMP           TIMESTAMP(6),
    DEACTIVATION_DATE  DATE,
    CHANGE_STATUS_FLAG CHAR(1),
    STATUS             CHAR(1),
    SHORT_DESCR        VARCHAR(15),
    DESCR              VARCHAR(30),
    IAS_FLAG           VARCHAR(1) default ' '
);

create unique index IXU_GLG_080
    on GLG_JOURNAL (JOURNAL_ID);

