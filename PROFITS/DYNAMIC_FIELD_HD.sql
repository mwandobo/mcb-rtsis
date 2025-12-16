create table DYNAMIC_FIELD_HD
(
    FIELD_TYPE  CHAR(20) not null
        constraint PRKID_DYNAMIC_FIELD
            primary key,
    DESCRIPTION CHAR(100)
);

