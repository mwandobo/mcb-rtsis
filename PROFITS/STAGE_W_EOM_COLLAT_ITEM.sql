create table STAGE_W_EOM_COLLAT_ITEM
(
    EOM_DATE        DATE,
    COMBO_KEY       CHAR(12),
    TYPE_KEY        CHAR(2),
    INTERNAL_SN     DECIMAL(10),
    RECORD_TYPE_IND CHAR(15) default 'n/a'
);

