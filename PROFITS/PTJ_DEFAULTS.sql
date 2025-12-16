create table PTJ_DEFAULTS
(
    ID_JUSTIFIC    INTEGER,
    ID_PRODUCT     INTEGER,
    SECONDARY_SWIN CHAR(8) not null,
    PRIMARY_SWIN   CHAR(8) not null,
    constraint PK_PTJ_DEFAULTS
        primary key (PRIMARY_SWIN, SECONDARY_SWIN)
);

