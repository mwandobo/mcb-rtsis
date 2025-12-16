create table GLG_CLASS
(
    CLASS_ID          CHAR(1),
    DEACTIVATION_DATE DATE,
    TIMESTMP          TIMESTAMP(6),
    GROUP_IND         CHAR(1),
    STATUS            CHAR(1),
    SHORT_DESCR       CHAR(20),
    DESCR             VARCHAR(50)
);

create unique index IXU_GLG_056
    on GLG_CLASS (CLASS_ID);

