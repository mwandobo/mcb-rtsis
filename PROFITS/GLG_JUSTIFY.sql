create table GLG_JUSTIFY
(
    JUSTIFY_ID         CHAR(4),
    TIMESTMP           TIMESTAMP(6),
    DEACTIVATION_DATE  DATE,
    VALIDITY_DATE      DATE,
    TYPE               CHAR(1),
    CHANGE_STATUS_FLAG CHAR(1),
    SPECIAL_TYPE_FLG   CHAR(1),
    STATUS             CHAR(1),
    DESCR              VARCHAR(40)
);

create unique index IXU_GLG_081
    on GLG_JUSTIFY (JUSTIFY_ID);

