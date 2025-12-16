create table SWIFT_FIELD_FORMATS
(
    INTERNAL_SN    INTEGER not null,
    TAG_FIELD_TYPE CHAR(2) not null,
    FIELD_FORMAT   CHAR(100),
    constraint IXU_FX_043
        primary key (INTERNAL_SN, TAG_FIELD_TYPE)
);

