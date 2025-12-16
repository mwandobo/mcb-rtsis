create table TEKE_S_A
(
    FK_CUSTOMERCUST_ID      INTEGER not null,
    SUM_EURO_AMOUNT_PORTION DECIMAL(15, 2),
    BLOCKED                 VARCHAR(1),
    BLOCKED_DEP_AMT         DECIMAL(15, 2),
    AVAILABLE_DEP_AMT       DECIMAL(15, 2),
    SUM_OV_ACC_BOOK_BAL     DECIMAL(15, 2),
    DIFF_AMT                DECIMAL(15, 2),
    DAMAGE_AMT              DECIMAL(15, 2)
);

