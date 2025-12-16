create table TMP_ISOZYGIO
(
    ROW_ID      INTEGER  not null,
    ACCOUNT_NO  CHAR(20) not null,
    DESCRIPTION VARCHAR(100),
    UNITCODE    INTEGER  not null,
    CCY         VARCHAR(5),
    DR_AMT      VARCHAR(100),
    CR_AMT      VARCHAR(100)
);

