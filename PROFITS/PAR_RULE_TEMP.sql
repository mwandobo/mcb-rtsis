create table PAR_RULE_TEMP
(
    TMSTAMP            TIMESTAMP(6) not null,
    ID                 INTEGER      not null,
    SNUM               SMALLINT     not null,
    CURRENCY_ROUND_FLG CHAR(1),
    SOURCE_RATE_TYPE   CHAR(1),
    TARGET_RATE_TYPE   CHAR(1),
    TARGET_AMOUNT      CHAR(18),
    AMOUNT_DESCRIPTION CHAR(15),
    FIRST_AMOUNT       CHAR(18),
    FUNCTION           CHAR(2),
    SECOND_AMOUNT      CHAR(18),
    EXTRAIT_SEQ        SMALLINT,
    ADVICE_SEQ         SMALLINT,
    EXTRAIT_AMN_TYPE   CHAR(2),
    CHARGE_IND         CHAR(1),
    CHARGE_ID          INTEGER,
    CURRENCY_ID        INTEGER,
    CURR_SOURCE        CHAR(1),
    DC_IND             CHAR(1),
    EXTRAIT_TABLE_IND  CHAR(7),
    USR_TOTAL_IND      CHAR(1),
    ROUNDING_IND       CHAR(1),
    GO_TO_LINE         SMALLINT,
    FX_POSITION_IND    CHAR(1),
    FB_POSITION_IND    CHAR(1),
    VALEUR_UPD         CHAR(1),
    VALEUR_INIT_FLG    CHAR(1),
    constraint IXU_REP_019
        primary key (TMSTAMP, ID, SNUM)
);

