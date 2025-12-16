create table CMS_PARAMS
(
    PVKI                 CHAR(1),
    PAN_TYPE             CHAR(3),
    FK_CRDTYP_GENERIC_HD CHAR(5) not null,
    FK_CRDTYP_GENERIC_DT INTEGER not null,
    SWITCH_CODE          CHAR(4),
    EXP_ACC_OBLIG_IND    CHAR(1),
    VIRTUAL_CARD_FLG     CHAR(1),
    constraint PK_CMS_PARAMS
        primary key (FK_CRDTYP_GENERIC_HD, FK_CRDTYP_GENERIC_DT)
);

