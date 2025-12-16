create table AGR_GL_AGREEMENT
(
    ID_PRODUCT  INTEGER not null,
    UNIT_CODE   INTEGER not null,
    ID_CURRENCY INTEGER not null,
    TOTAL_LIMIT DECIMAL(15, 2),
    constraint IXU_LNS_030
        primary key (ID_PRODUCT, UNIT_CODE, ID_CURRENCY)
);

