create table ISS_COMM_PACK
(
    TP_SO_IDENTIFIER DECIMAL(10),
    TRX_DATE         DATE,
    TRX_SN           INTEGER,
    ID_PACKAGE       INTEGER
);

create unique index IXU_ISS_021
    on ISS_COMM_PACK (TP_SO_IDENTIFIER, TRX_DATE, TRX_SN);

