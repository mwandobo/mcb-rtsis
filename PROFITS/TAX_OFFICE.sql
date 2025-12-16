create table TAX_OFFICE
(
    ID                 SMALLINT,
    FKGD_HAS_COUNTRY_N INTEGER,
    TMSTAMP            TIMESTAMP(6),
    ENTRY_STATUS       CHAR(1),
    DOM_TAX_OFFICE     CHAR(1),
    ZIPCODE            CHAR(5),
    FKGH_HAS_COUNTRY_N CHAR(5),
    FAX                VARCHAR(10),
    CITY               VARCHAR(15),
    TELEPHONE          VARCHAR(15),
    TAX_OFFICE_NAME    VARCHAR(20),
    ADDRESS            VARCHAR(40)
);

create unique index IXU_TAX_002
    on TAX_OFFICE (ID);

