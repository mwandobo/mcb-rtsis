create table W_CODE
(
    CODE_SET_ID DECIMAL(10),
    CODE_VALUE  VARCHAR(6),
    CODE_DESC   VARCHAR(50)
);

create unique index PK_W_CODE
    on W_CODE (CODE_SET_ID, CODE_VALUE);

