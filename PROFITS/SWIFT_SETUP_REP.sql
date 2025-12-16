create table SWIFT_SETUP_REP
(
    MESSAGE_TYPE CHAR(20) not null,
    TAG          CHAR(10) not null,
    SUBTAG_SN    SMALLINT not null,
    MESSAGE_SN   INTEGER  not null,
    MSG_CATEGORY CHAR(1)  not null,
    TAG_LABEL    CHAR(40),
    SHOW_ORDER   INTEGER,
    constraint PK_SWIFT_REP
        primary key (MESSAGE_TYPE, TAG, SUBTAG_SN, MESSAGE_SN)
);

