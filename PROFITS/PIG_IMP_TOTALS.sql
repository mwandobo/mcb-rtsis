create table PIG_IMP_TOTALS
(
    FK_HAS_HDASCDAT_TP CHAR(1)     not null,
    FK_HAS_HDASCDAT_SN DECIMAL(10) not null,
    UNIT_CODE          INTEGER     not null,
    ACT_AMOUNT04       DECIMAL(15, 2),
    ACT_AMOUNT05       DECIMAL(15, 2),
    MAN_AMOUNT02       DECIMAL(15, 2),
    MAN_AMOUNT03       DECIMAL(15, 2),
    MAN_AMOUNT04       DECIMAL(15, 2),
    MAN_AMOUNT05       DECIMAL(15, 2),
    ACT_AMOUNT03       DECIMAL(15, 2),
    ACT_AMOUNT02       DECIMAL(15, 2),
    ACT_AMOUNT01       DECIMAL(15, 2),
    MAN_AMOUNT01       DECIMAL(15, 2),
    constraint IXU_PRD_015
        primary key (FK_HAS_HDASCDAT_TP, FK_HAS_HDASCDAT_SN, UNIT_CODE)
);

