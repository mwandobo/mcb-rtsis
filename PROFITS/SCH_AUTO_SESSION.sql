create table SCH_AUTO_SESSION
(
    FK_AUTOSETUP_ID VARCHAR(40)  not null,
    FK_SCRIPT_ID    VARCHAR(40)  not null,
    FK_SESSION_ID   TIMESTAMP(6) not null,
    LOG             CLOB(1048576),
    constraint SCH_AUTO_SESSION_PK
        primary key (FK_AUTOSETUP_ID, FK_SCRIPT_ID, FK_SESSION_ID)
);

