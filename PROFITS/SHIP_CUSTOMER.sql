create table SHIP_CUSTOMER
(
    INTERNAL_SN      DECIMAL(10) not null,
    ENTRY_STATUS     CHAR(1),
    OWNERSH_PERC     DECIMAL(5, 2),
    OWNERSH_DATE     DATE,
    FK_CUSTID        INTEGER     not null,
    FK_SHIPID        DECIMAL(10) not null,
    FK_GH_SH_OWNTYPE CHAR(5),
    FK_GD_SH_OWNTYPE INTEGER,
    constraint PK_SHPCUST
        primary key (FK_CUSTID, FK_SHIPID, INTERNAL_SN)
);

create unique index I0000896
    on SHIP_CUSTOMER (FK_CUSTID);

create unique index I0000899
    on SHIP_CUSTOMER (FK_SHIPID);

create unique index I0000917
    on SHIP_CUSTOMER (FK_GH_SH_OWNTYPE, FK_GD_SH_OWNTYPE);

