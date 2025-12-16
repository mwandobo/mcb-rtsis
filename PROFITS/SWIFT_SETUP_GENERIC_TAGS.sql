create table SWIFT_SETUP_GENERIC_TAGS
(
    TAG_SERIAL_NUM  INTEGER     not null
        constraint PK_INTERNAL_SN
            primary key,
    TAG             VARCHAR(50) not null,
    TAG_DESCRIPTION VARCHAR(100)
);

