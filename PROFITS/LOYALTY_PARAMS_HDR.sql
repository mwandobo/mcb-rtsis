create table LOYALTY_PARAMS_HDR
(
    TAG_SET_CODE        CHAR(20) not null,
    TAG                 CHAR(10) not null,
    DESCRIPTION         VARCHAR(150),
    MIN_VALUE           DECIMAL(15, 2),
    MIN_OPERATOR        CHAR(3),
    MAX_VALUE           DECIMAL(15, 2),
    MAX_OPERATOR        CHAR(3),
    MAX_LOYAL_POINTS    INTEGER,
    WEIGHT              DECIMAL(6, 2),
    ENTRY_STATUS        CHAR(1),
    INCLUDE_IN_TOTALS   CHAR(1),
    GRAND_TOTAL_TAG_FLG CHAR(1),
    constraint LOYAL_PARAMS_HDR
        primary key (TAG_SET_CODE, TAG)
);

