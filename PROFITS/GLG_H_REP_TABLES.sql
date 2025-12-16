create table GLG_H_REP_TABLES
(
    TABLE_ID CHAR(2),
    TIMESTMP DATE,
    STATUS   CHAR(1),
    DESCR    VARCHAR(50)
);

create unique index IXU_GLG_077
    on GLG_H_REP_TABLES (TABLE_ID);

