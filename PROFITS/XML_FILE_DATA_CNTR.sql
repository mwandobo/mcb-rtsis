create table XML_FILE_DATA_CNTR
(
    XML_COUNTER DECIMAL(12) generated always as identity
        constraint PK_XML_FILE_DARA_CNTR
            primary key,
    TMSTAMP     TIMESTAMP(6)
);

