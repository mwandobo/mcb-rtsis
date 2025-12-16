create table LNS_BUDGET_TOTALS
(
    TMSTAMP            DATE,
    UNIT               INTEGER,
    PRODUCT            INTEGER,
    ACC_CD             SMALLINT,
    ACC_TYPE           SMALLINT,
    ACC_UNIT           INTEGER,
    ACC_SN             INTEGER,
    DB_INTEREST_AMN    DECIMAL(15, 2),
    N128_INTEREST_AMN  DECIMAL(15, 2),
    SPREAD_INEREST_AMN DECIMAL(15, 2),
    TOKARITMOS_AMN     DECIMAL(15, 2),
    TOTAL_INTER_AMN    DECIMAL(15, 2),
    END_DATE           DATE,
    STARTING_DATE      DATE,
    TYPE               CHAR(1),
    LOAN_STATUS        CHAR(1),
    ACCOUNTED          CHAR(1)
);

create unique index IXU_LNS_010
    on LNS_BUDGET_TOTALS (TMSTAMP, UNIT, PRODUCT);

