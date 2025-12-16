create table DCD_EXITST_TRANSL
(
    ID              DECIMAL(12) not null,
    LANGUAGE_USED   INTEGER     not null,
    PRFT_SYSTEM     SMALLINT    not null,
    EXIT_STATE_DESC CHAR(40),
    ACTUAL_MESSAGE  VARCHAR(2048),
    constraint IXU_DEF_045
        primary key (ID, LANGUAGE_USED, PRFT_SYSTEM)
);

