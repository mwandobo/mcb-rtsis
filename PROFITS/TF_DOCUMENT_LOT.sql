create table TF_DOCUMENT_LOT
(
    FK_TRADEFIN_NUM     CHAR(40) not null,
    LOT_SN              SMALLINT not null,
    FK1GEN_DET_SN       INTEGER,
    FK_GEN_DET_SN       INTEGER,
    FK0GEN_DET_SN       INTEGER,
    RECEIPT_DATE        DATE,
    NOTIFICATION_STATUS CHAR(1),
    LOT_STATUS          CHAR(1),
    FK_GEN_DET_GEN_HD   CHAR(5),
    FK1GEN_DET_GEN_HD   CHAR(5),
    FK0GEN_DET_GEN_HD   CHAR(5),
    DUE_DATE            DATE,
    constraint IXU_FX_046
        primary key (FK_TRADEFIN_NUM, LOT_SN)
);

