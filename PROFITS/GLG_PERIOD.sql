create table GLG_PERIOD
(
    PERIOD_ID          SMALLINT,
    YEAR0              SMALLINT,
    FROM_DATE          DATE,
    TIMESTMP           TIMESTAMP(6),
    DEACTIVATION_DATE  DATE,
    TO_DATE            DATE,
    TRN_FLAG           CHAR(1),
    STATUS             CHAR(1),
    CHANGE_STATUS_FLAG CHAR(1),
    DESCR              VARCHAR(15)
);

create unique index IXU_GLG_085
    on GLG_PERIOD (PERIOD_ID, YEAR0);

