create table PAR_RELATION_DETAIL
(
    FK_PAR_RELATIONCOD CHAR(8)      not null,
    FKGH_HAS_A_PRIMARY CHAR(5)      not null,
    FKGD_HAS_A_PRIMARY INTEGER      not null,
    FKGH_HAS_A_SECONDA CHAR(5)      not null,
    FKGD_HAS_A_SECONDA INTEGER      not null,
    ENTRY_STATUS       CHAR(1)      not null,
    TMSTAMP            TIMESTAMP(6) not null,
    constraint PKPARELD
        primary key (FKGH_HAS_A_SECONDA, FKGD_HAS_A_SECONDA, FKGH_HAS_A_PRIMARY, FKGD_HAS_A_PRIMARY, FK_PAR_RELATIONCOD)
);

