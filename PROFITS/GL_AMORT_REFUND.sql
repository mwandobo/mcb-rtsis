create table GL_AMORT_REFUND
(
    ARTICLE_SN         DECIMAL(15) not null,
    REFUND_SN          DECIMAL(5)  not null,
    REFUND_INTERNAL_SN DECIMAL(5)  not null,
    TRANSACTION_TYPE   CHAR(1),
    DB_CR_FLG          CHAR(1),
    PROF_ACC_NUM       CHAR(40),
    BAL_GL_ACCOUNT     CHAR(21),
    PROF_LOSS_GL_ACC   CHAR(21),
    REFUND_AMOUNT      DECIMAL(18, 2),
    I_RATE             DECIMAL(12, 6),
    DOMESTIC_AMOUNT    DECIMAL(18, 2),
    CURRENCY           DECIMAL(5),
    JUSTIFICATION      CHAR(80),
    TIMESTAMP          TIMESTAMP(6),
    TRX_DATE           DATE,
    TRX_UNIT           DECIMAL(5),
    TRX_USER           CHAR(8),
    TRX_USER_SN        DECIMAL(8),
    TRX_INTERNAL_SN    DECIMAL(3),
    FILE_NAME          CHAR(20),
    AUTH_FILE_STATUS   CHAR(1),
    constraint GL_AMORT_REFUND_PK
        primary key (ARTICLE_SN, REFUND_SN, REFUND_INTERNAL_SN)
);

