create table ELTA_BARCODES
(
    SERIAL_NO      INTEGER     not null,
    BARCODE        VARCHAR(50) not null,
    TEXT_01        VARCHAR(100),
    TEXT_02        VARCHAR(100),
    TEXT_03        VARCHAR(100),
    TEXT_04        VARCHAR(100),
    TEXT_05        VARCHAR(100),
    TEXT_06        VARCHAR(100),
    TEXT_07        VARCHAR(100),
    TEXT_08        VARCHAR(100),
    TEXT_09        VARCHAR(100),
    TEXT_10        VARCHAR(100),
    ENTRY_STATUS   CHAR(1),
    CREATE_TMSTAMP TIMESTAMP(6),
    primary key (SERIAL_NO, BARCODE)
);

