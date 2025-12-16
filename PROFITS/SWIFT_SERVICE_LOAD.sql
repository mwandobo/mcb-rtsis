create table SWIFT_SERVICE_LOAD
(
    FK_SERVC_SYS_PARAM CHAR(5)     not null,
    FK_SERVC_SYS_NUM   INTEGER     not null,
    SN                 DECIMAL(10) not null,
    HEADER_KEYWORD     VARCHAR(100),
    MESSAGE_TYPE       CHAR(20)    not null,
    TAG                VARCHAR(10),
    TAG_ALLOWED_VALUE  VARCHAR(100),
    TAG_LINE           INTEGER,
    LOAD_UNPROCESSED   CHAR(1),
    constraint PK_SWT_SERV_LOAD
        primary key (FK_SERVC_SYS_PARAM, FK_SERVC_SYS_NUM, MESSAGE_TYPE, SN)
);

