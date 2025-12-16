create table CURR_TABLE
(
    FK_CURR_RATE_TYTYP CHAR(3),
    TABLE_NUM          INTEGER not null
        constraint I0000666
            primary key,
    ISSUE_DATE         DATE,
    GEN_DETAIL         INTEGER,
    TIMESTMP           TIMESTAMP(6),
    STATUS             CHAR(1),
    GEN_HEADER         CHAR(5),
    DESCR              VARCHAR(30),
    ISSUE_TIME         TIME
);

create unique index IXU_CURRTBL000
    on CURR_TABLE (TABLE_NUM, ISSUE_DATE, ISSUE_TIME);

