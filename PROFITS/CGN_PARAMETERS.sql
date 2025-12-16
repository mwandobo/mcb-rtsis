create table CGN_PARAMETERS
(
    SN              DECIMAL(10) not null
        constraint PK_CGN_PARAM
            primary key,
    PLAN_ID_TYPE    CHAR(2)     not null,
    SQL_VIEW_CREATE VARCHAR(80),
    SQL_VIEW_SELECT VARCHAR(80),
    SQL_VIEW_DROP   VARCHAR(80),
    RESET_CUST_SEQ  CHAR(80),
    RESET_ACC_SEQ   CHAR(80),
    FUNC_SBSTR      VARCHAR(20)
);

