create table IPS_REMITTANCE_CODE_LINK
(
    ORGANIZATION_CODE   INTEGER default 0 not null,
    REMITTANCE_CODE     CHAR(20)          not null,
    REMITTANCE_CODE_NEW CHAR(20)          not null,
    REMITTANCE_CODE_OLD CHAR(20)          not null,
    constraint IPS_REMITTANCE_CODE_LINK_PK
        primary key (REMITTANCE_CODE, ORGANIZATION_CODE)
);

