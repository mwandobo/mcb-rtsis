create table PERMIT_VALUE
(
    ID             DECIMAL(10)  not null
        constraint I0000371
            primary key,
    LOW_VALUE      VARCHAR(512) not null,
    HIGH_VALUE     VARCHAR(512) not null,
    FK_ATTRIBUTEID DECIMAL(10)
);

create unique index I0000531
    on PERMIT_VALUE (FK_ATTRIBUTEID);

