create table GLG_CURR_CATEG
(
    CURR_CATEG_ID      CHAR(4),
    TIMESTMP           TIMESTAMP(6),
    VALIDITY_DATE      DATE,
    DEACTIVATION_DATE  DATE,
    CHANGE_STATUS_FLAG CHAR(1),
    STATUS             CHAR(1),
    DESCR              VARCHAR(30)
);

create unique index IXU_GLG_058
    on GLG_CURR_CATEG (CURR_CATEG_ID);

