create table COUNTRY_HLD
(
    DATE_ID        DATE    not null,
    DESCRIPTION    VARCHAR(40),
    FK_COUNTRY_GH  CHAR(5) not null,
    FK_COUNTRY_GD  INTEGER not null,
    FK_CURRENCY_ID INTEGER not null,
    FK_SERVICE_GH  CHAR(5) not null,
    FK_SERVICE_GD  INTEGER not null,
    constraint PK_SERVICE_HLD
        primary key (FK_COUNTRY_GH, FK_COUNTRY_GD, DATE_ID, FK_CURRENCY_ID, FK_SERVICE_GH, FK_SERVICE_GD)
);

comment on column COUNTRY_HLD.DATE_ID is 'It is the official holiday''s date, that identifies with the currency the Official_Hld entity.';

comment on column COUNTRY_HLD.DESCRIPTION is 'It is a text describing the official holiday (e.g Christmas).';

create unique index I0000466
    on COUNTRY_HLD (FK_COUNTRY_GH, FK_COUNTRY_GD);

create unique index I0010468
    on COUNTRY_HLD (FK_CURRENCY_ID);

create unique index I0010470
    on COUNTRY_HLD (FK_SERVICE_GH, FK_SERVICE_GD);

