create table GLG_H_FISCFORM
(
    FORM_ID           CHAR(2),
    TIMESTMP          TIMESTAMP(6),
    DEACTIVATION_DATE DATE,
    STATUS            CHAR(1),
    DESCR_LINE_2      VARCHAR(50),
    DESCR             VARCHAR(50),
    DESCR_LINE_3      VARCHAR(50)
);

create unique index IXU_GLG_076
    on GLG_H_FISCFORM (FORM_ID);

