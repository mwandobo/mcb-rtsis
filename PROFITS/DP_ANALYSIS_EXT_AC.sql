create table DP_ANALYSIS_EXT_AC
(
    CUST_TYPE     CHAR(1) not null,
    RESIDENT_FLAG CHAR(1) not null,
    COMP_SN       INTEGER not null,
    KATAG_SN      INTEGER not null,
    COUNTER       INTEGER,
    TOT_AMT       DECIMAL(15, 2),
    TOT_COMP_AMT  DECIMAL(15, 2),
    constraint IXU_DEP_132
        primary key (CUST_TYPE, RESIDENT_FLAG, COMP_SN, KATAG_SN)
);

