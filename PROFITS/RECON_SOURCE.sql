create table RECON_SOURCE
(
    SN              INTEGER not null
        constraint PK_RECON_2
            primary key,
    SOURCE_DESC     VARCHAR(80),
    SOURCE_ANALYSIS VARCHAR(4000)
);

