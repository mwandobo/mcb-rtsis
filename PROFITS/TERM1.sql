create table TERM1
(
    "CUSTOMER CODE"        SMALLINT,
    "ACCOUNT CD"           SMALLINT,
    "DURATION PERIOD"      SMALLINT,
    FK_GENERIC_DETASER     INTEGER,
    "CUSTOMER CATEGORY"    INTEGER,
    "ACTIVITY SECTOR"      INTEGER,
    "CURRENCY CODE"        INTEGER,
    "PRODUCT ID"           INTEGER,
    "CUSTOMER ID"          INTEGER,
    CR_INTEREST_RATE       DECIMAL(8, 4),
    I_ACC                  DECIMAL(11),
    "BOOK BALANCE"         DECIMAL(15, 2),
    "ACCRUALS INTEREST CR" DECIMAL(15, 2),
    DURATION_DAYS          DECIMAL(15, 2),
    DAYS                   DECIMAL(15, 2),
    CR_INT_GL_ACC          CHAR(21),
    CR_INT_ACCR_GL_ACC     CHAR(21),
    GL_ACCOUNT             CHAR(21),
    "ACCOUNT NO"           CHAR(40)
);

