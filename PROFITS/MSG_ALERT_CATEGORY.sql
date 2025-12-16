create table MSG_ALERT_CATEGORY
(
    CR_DB_FLAG   CHAR(1) not null,
    LOGICAL      CHAR(1) not null,
    TYPE         CHAR(3) not null,
    DISPLAY_TEXT CHAR(60),
    constraint IXU_CIS_174
        primary key (CR_DB_FLAG, LOGICAL, TYPE)
);

