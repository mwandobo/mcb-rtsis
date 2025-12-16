create table GLG_CNFTAX
(
    TAX_TYPE           CHAR(4),
    LINE_NUM           CHAR(4),
    RATE               DECIMAL(5, 2),
    TOLERANCE_AMNT     DECIMAL(6, 2),
    TIMESTMP           TIMESTAMP(6),
    DEACTIVATION_DATE  DATE,
    MVMNT_TYPE         CHAR(1),
    CHECK_FLAG         CHAR(1),
    STATUS             CHAR(1),
    CHANGE_STATUS_FLAG CHAR(1),
    FK_GLG_ACCOUNTACCO CHAR(21),
    FK_GLG_ACCOUNTACC0 CHAR(21),
    DESCRIPTION        CHAR(40)
);

create unique index IXU_GLG_057
    on GLG_CNFTAX (TAX_TYPE, LINE_NUM);

