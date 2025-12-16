create table DOCUMENT_MANAGEMENT
(
    REFERENCE_NUMBER VARCHAR(80) not null,
    PRFT_SYS         SMALLINT    not null,
    SCAN_SN          DECIMAL(12),
    FILE_SN          DECIMAL(15),
    constraint PK_DOC_MNGMNT
        primary key (PRFT_SYS, REFERENCE_NUMBER)
);

