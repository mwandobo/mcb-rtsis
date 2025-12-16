create table STAGE_W_DIM_DATE
(
    DATE_KEY               DECIMAL(10),
    FULL_DATE              DATE,
    DAY_NAME               CHAR(3),
    HOLIDAY_INDICATOR      VARCHAR(11),
    YEAR_MONTH             INTEGER,
    YEAR_MINUS_1_LAST_DATE DATE,
    PRODUCTION_DATE        DATE
);

