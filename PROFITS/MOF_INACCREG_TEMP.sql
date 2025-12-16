create table MOF_INACCREG_TEMP
(
    MOF_REQUEST_ID  CHAR(12) not null
        constraint PK_MOF_INACCREG_TEMP
            primary key,
    PRFT_CUST_ID    INTEGER,
    MOF_RESULT_CODE CHAR(4)
);

