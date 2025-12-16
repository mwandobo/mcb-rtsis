create table BDG_PERIOD
(
    CHANGE_STATUS_FLAG CHAR(1),
    STATUS             CHAR(1),
    TRN_FLAG           CHAR(1),
    DEACTIVATION_DATE  DATE,
    TIMESTMP           TIMESTAMP(6),
    DATE_TO            DATE,
    FROM_DATE          DATE,
    DESCRIPTION        CHAR(50),
    PERIOD_ID          VARCHAR(5) not null,
    YEAR0              SMALLINT   not null,
    constraint IXU_BDG_PER
        primary key (YEAR0, PERIOD_ID)
);

