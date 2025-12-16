create table BDG_DISTRICT
(
    ID                CHAR(3) not null
        constraint IXU_BDG_002
            primary key,
    STATUS            SMALLINT,
    TMSTMP            TIMESTAMP(6),
    FK_BDG_DISTRICTID CHAR(4),
    DESCRIPTION       VARCHAR(30)
);

create unique index I0000505
    on BDG_DISTRICT (FK_BDG_DISTRICTID);

