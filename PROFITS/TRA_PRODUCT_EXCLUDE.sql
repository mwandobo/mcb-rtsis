create table TRA_PRODUCT_EXCLUDE
(
    ID_PRODUCT   INTEGER not null
        constraint PK_TRA_PR_EXCLD
            primary key,
    PRODUCT_DESC VARCHAR(500),
    ENTRY_STATUS CHAR(1),
    TMSTAMP      TIMESTAMP(6)
);

comment on table TRA_PRODUCT_EXCLUDE is 'Table which will define which Products should be completely excluded from the process of TRA';

