create table GENERIC_CATEGORY
(
    GEN_CAT_CODE       DECIMAL(15),
    SHOW_FLAG          CHAR(1),
    OWNER_SYSTEM       CHAR(3),
    DESCRIPTION        CHAR(40),
    SECOND_DESCRIPTION VARCHAR(50),
    TMSTAMP            TIMESTAMP(6),
    FK_GENERIC_HEADPAR CHAR(5),
    REF_DATE           DATE,
    TAX_REG_NO         CHAR(9),
    PROFESSION         CHAR(15),
    MYF_LIABLE_FLAG    CHAR(1),
    CITY_ADDR          CHAR(10),
    STREET_ADDR        CHAR(16),
    STREET_NR_ADDR     CHAR(3),
    POST_CODE          INTEGER
);

