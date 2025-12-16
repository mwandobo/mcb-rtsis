create table SCAN_IMAGE_CNTR
(
    IMAGE_COUNTER DECIMAL(12) generated always as identity
        constraint PK_SCAN_IMAGE_CNTR
            primary key,
    TMSTAMP       TIMESTAMP(6)
);

