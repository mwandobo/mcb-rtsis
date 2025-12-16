create table MONTRAN_OUT_SUM
(
    FILE_NAME          CHAR(50) not null
        constraint IXU_FX_019
            primary key,
    KOL                SMALLINT,
    SUM                DECIMAL(15, 2),
    PRFT_EXTRACTED_DT  TIMESTAMP(6),
    PRFT_CREATED_DT    TIMESTAMP(6),
    PRFT_EXTRACTED_FLG CHAR(1),
    POR                CHAR(3),
    MDATE              CHAR(6),
    NFA                CHAR(9)
);

