create table BOP_TRX_RELATION
(
    TRX_CODE     INTEGER not null,
    ID_PRODUCT   INTEGER not null,
    ID_JUSTIFIC  INTEGER not null,
    TRX_CATEGORY CHAR(5) not null,
    CR_DR_FLG    CHAR(1),
    constraint IXU_FX_042
        primary key (TRX_CODE, ID_PRODUCT, ID_JUSTIFIC, TRX_CATEGORY)
);

