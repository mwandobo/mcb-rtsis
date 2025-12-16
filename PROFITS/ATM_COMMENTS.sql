create table ATM_COMMENTS
(
    USR_CODE CHAR(8) not null
        constraint PK_ATM_COMM
            primary key,
    COMMENTS VARCHAR(40)
);

