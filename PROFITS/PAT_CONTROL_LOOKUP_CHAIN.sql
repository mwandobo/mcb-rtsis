create table PAT_CONTROL_LOOKUP_CHAIN
(
    ID                  DECIMAL(10) not null
        constraint PATWCHPK
            primary key,
    INDEX0              SMALLINT    not null,
    WINDOW_CLASS_NAME   CHAR(254)   not null,
    FK_PAT_WINCONTRUID0 DECIMAL(10)
);

create unique index PATWCHI1
    on PAT_CONTROL_LOOKUP_CHAIN (FK_PAT_WINCONTRUID0);

