create table CORRESPONDENT_HLD
(
    FK_CUSTOMERCUST_ID INTEGER not null,
    DATE_ID            DATE    not null,
    DESCRIPTION        CHAR(40),
    constraint IXU_COR_000
        primary key (FK_CUSTOMERCUST_ID, DATE_ID)
);

