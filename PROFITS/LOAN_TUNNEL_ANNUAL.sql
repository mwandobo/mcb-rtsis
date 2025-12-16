create table LOAN_TUNNEL_ANNUAL
(
    YEAR_SN           SMALLINT,
    TRX_USER          CHAR(8),
    TMSTAMP           TIMESTAMP(6),
    ACC_CD            SMALLINT,
    INSTALL_PER_YEAR  SMALLINT,
    ACC_TYPE          SMALLINT,
    ACC_UNIT          INTEGER,
    ACC_SN            INTEGER,
    ANNUAL_CAP_AMN    DECIMAL(15, 2),
    REMAINING_CAP_AMN DECIMAL(15, 2),
    START_DT          DATE,
    END_DT            DATE
);

create unique index IXU_LOA_012
    on LOAN_TUNNEL_ANNUAL (YEAR_SN, TRX_USER, TMSTAMP);

