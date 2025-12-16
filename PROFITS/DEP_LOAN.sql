create table DEP_LOAN
(
    FK_PRODUCTID_PRODU INTEGER not null
        constraint PKLOAN0
            primary key,
    FORGN_CURRENCY     CHAR(1),
    PRODUCT_ID         INTEGER
);

