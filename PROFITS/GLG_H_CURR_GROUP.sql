create table GLG_H_CURR_GROUP
(
    GROUP_ID           CHAR(4),
    TIMESTMP           TIMESTAMP(6),
    VALIDITY_DATE      DATE,
    DEACTIVATION_DATE  DATE,
    CHANGE_STATUS_FLAG CHAR(1),
    STATUS             CHAR(1),
    DESCR              VARCHAR(10),
    CLASS_FLAG         CHAR(1) default '0'
);

create unique index IXU_GLG_075
    on GLG_H_CURR_GROUP (GROUP_ID);

