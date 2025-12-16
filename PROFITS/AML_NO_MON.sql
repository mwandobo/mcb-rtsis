create table AML_NO_MON
(
    LF_TIMESTAMP       TIMESTAMP(6) not null,
    LF_ACTION          CHAR(32)     not null,
    PROC_FLAG          CHAR(1),
    LF_ACC_CURRENCYISO CHAR(3),
    LF_INSTITUTE       CHAR(4),
    LF_BUSINESSTYPE    CHAR(4),
    LF_ACCNO           CHAR(11),
    LF_BUSINESSNO      CHAR(11),
    LF_EMPL_NO         CHAR(16),
    LF_CUSTNO          CHAR(16),
    LF_CHANGE          CHAR(18),
    LF_ACTION_CODE     CHAR(32),
    constraint IXU_AML_006
        primary key (LF_TIMESTAMP, LF_ACTION)
);

