create table ENCYMODEL
(
    ID        DECIMAL(10)  not null,
    MODEL     VARCHAR(100) not null,
    GOOD      SMALLINT,
    ENCY      INTEGER      not null,
    MODELCODE CHAR(3)
);

create unique index PK_ENCYIND
    on ENCYMODEL (MODEL);

create unique index PK_ENCYMODEL
    on ENCYMODEL (ID);

