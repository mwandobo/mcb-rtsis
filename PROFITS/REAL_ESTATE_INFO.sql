create table REAL_ESTATE_INFO
(
    ENTRY_STATUS       CHAR(1),
    FK_REAL_ESTATEID   DECIMAL(10) not null,
    FK_GH_HAS_ADD_INFO CHAR(5)     not null,
    FK_GD_HAS_ADD_INFO INTEGER     not null,
    constraint PK_REAL_EST_INFO
        primary key (FK_REAL_ESTATEID, FK_GH_HAS_ADD_INFO, FK_GD_HAS_ADD_INFO)
);

