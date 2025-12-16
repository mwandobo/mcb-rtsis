create table GLG_H_TRANSITION
(
    TRANS_ID        CHAR(4),
    CONVERTION_CURR INTEGER,
    TIMESTMP        DATE,
    STATUS          CHAR(1),
    SAME_CURR_FLG   CHAR(1),
    USES_CURR_GROUP CHAR(4),
    SECOND_JUSTIFY  CHAR(4),
    FIRST_JUSTIFY   CHAR(4),
    DESCR           VARCHAR(50)
);

create unique index IXP_GLG_000
    on GLG_H_TRANSITION (TRANS_ID);

