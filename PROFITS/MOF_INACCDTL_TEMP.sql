create table MOF_INACCDTL_TEMP
(
    MOF_REQUEST_ID  CHAR(12) not null
        constraint PK_MOF_INACCDTL_TEMP
            primary key,
    PRFT_CUST_ID    INTEGER,
    MOF_RESULT_CODE CHAR(4),
    MOF_ACC_NUMBER  CHAR(27),
    ENTRY_STATUS    CHAR(1)
);

