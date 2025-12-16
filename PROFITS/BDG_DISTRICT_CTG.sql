create table BDG_DISTRICT_CTG
(
    ID          CHAR(4) not null
        constraint IXU_BDG_003
            primary key,
    STATUS      SMALLINT,
    TMSTMP      TIMESTAMP(6),
    DESCRIPTION VARCHAR(40)
);

