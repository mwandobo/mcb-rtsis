create table DP_PROD_EXTRAIT_CO
(
    PRODUCT_ID    INTEGER not null,
    VALIDITY_DATE DATE    not null,
    COMMENTS_1    CHAR(85),
    COMMENTS_2    CHAR(85),
    COMMENTS_3    CHAR(85),
    COMMENTS_4    CHAR(85),
    COMMENTS_5    CHAR(85),
    STATUS        CHAR(1),
    USER_CODE     CHAR(8),
    TMSTAMP       TIMESTAMP(6),
    constraint IXP_DPPRODEXT_01
        primary key (VALIDITY_DATE, PRODUCT_ID)
);

