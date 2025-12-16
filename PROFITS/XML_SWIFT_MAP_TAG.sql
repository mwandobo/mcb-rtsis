create table XML_SWIFT_MAP_TAG
(
    MAP_TYPE    CHAR(10)    not null,
    MT_MSG_TYPE CHAR(20)    not null,
    MT_TAG      VARCHAR(50) not null,
    MX_MSG_TYPE CHAR(20)    not null,
    MX_TAG      VARCHAR(50) not null,
    constraint PK_MT_TO_MX
        primary key (MAP_TYPE, MX_MSG_TYPE, MT_TAG, MT_MSG_TYPE, MX_TAG)
);

comment on table XML_SWIFT_MAP_TAG is 'Holds mapping for SWIFT MT tags mapping to SWIFT MX tags.';

create unique index IDX_MX_TO_MT
    on XML_SWIFT_MAP_TAG (MAP_TYPE, MX_MSG_TYPE, MX_TAG);

