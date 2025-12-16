create table XML_INP_FILE_COUNTER
(
    XML_COUNTER DECIMAL(12) generated always as identity
        constraint PK_XML_INP_FILE_CNTR
            primary key,
    TMSTAMP     TIMESTAMP(6)
);

