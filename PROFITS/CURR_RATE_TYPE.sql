create table CURR_RATE_TYPE
(
    TYPE_ID           CHAR(3),
    TIMESTMP          TIMESTAMP(6),
    STATUS            CHAR(1),
    FK_GLG_ENTEP_CTID CHAR(1),
    DESCR             VARCHAR(30)
);

create unique index IXU_CUR_005
    on CURR_RATE_TYPE (TYPE_ID);

