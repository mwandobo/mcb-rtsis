create table CORRESPONDENT_HL_U
(
    FK_CUSTOMERCUST_ID INTEGER not null,
    DATE_ID            DATE    not null,
    DESCRIPTION        CHAR(40),
    constraint IXU_CIU_010
        primary key (DATE_ID, FK_CUSTOMERCUST_ID)
);

