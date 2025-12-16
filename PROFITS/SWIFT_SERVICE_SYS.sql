create table SWIFT_SERVICE_SYS
(
    FK_SERVC_SYS_NUM   INTEGER not null,
    FK_SERVC_SYS_PARAM CHAR(5) not null,
    ID_TRANSACT        INTEGER not null,
    ID_PRODUCT         INTEGER not null,
    ID_JUSTIFIC        INTEGER not null,
    PROCESS_AS_STP     CHAR(1),
    constraint PK_SWT_SERVICE
        primary key (ID_TRANSACT, FK_SERVC_SYS_PARAM, FK_SERVC_SYS_NUM)
);

