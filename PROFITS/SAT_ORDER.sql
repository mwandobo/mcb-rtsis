create table SAT_ORDER
(
    SAT_ORDER_CODE     DECIMAL(15) not null
        constraint IXU_DEP_157
            primary key,
    ORDER_UNIT         INTEGER,
    FK_CUSTOMERCUST_ID INTEGER,
    ORDER_DATE         DATE,
    STATUS0            CHAR(1),
    PTS_USER_CODE      CHAR(10),
    CSD_ERROR          CHAR(10),
    PTS_ACCOUNT        CHAR(11),
    PTS_REGISTRY       CHAR(11),
    INVESTOR_ABBR      CHAR(15),
    ERROR_COMMENTS     VARCHAR(100)
);

