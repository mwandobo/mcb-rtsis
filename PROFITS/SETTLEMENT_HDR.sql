create table SETTLEMENT_HDR
(
    FILE_ID            DECIMAL(11) not null
        constraint PK_SETTLEMENT_HDR
            primary key,
    FILE_NAME          VARCHAR(40),
    DATE_OF_EXCHANGE   DATE,
    FILE_TYPE          VARCHAR(1),
    BANK               CHAR(2),
    CURRENCY_INDICATOR DECIMAL(1),
    CURRENCY_ISO_CODE  VARCHAR(3),
    FILE_COMPLETE_FLG  VARCHAR(1),
    HEADER_TEXT        VARCHAR(78),
    TRAILER_TEXT       VARCHAR(78),
    UPDATE_TIMESTAMP   TIMESTAMP(6)
);

comment on column SETTLEMENT_HDR.FILE_ID is 'The identity column. Auto increment number.';

comment on column SETTLEMENT_HDR.DATE_OF_EXCHANGE is 'The current date of PROFITS, on which the file was processed.';

comment on column SETTLEMENT_HDR.FILE_TYPE is 'The File Type. 0 for outward, 1 for incoming.';

comment on column SETTLEMENT_HDR.BANK is 'The Bank Clearing, i.e, Presenting Bank. Normally, our Bank.';

comment on column SETTLEMENT_HDR.CURRENCY_INDICATOR is 'Valeus are 0 = Local Currency, Uganda Shilling (UGX), 1=United States Dollar (USD), 2=Euro (EUR), 3=Great Britain Pound (GBP), 4=Kenya Shilling (KES)';

comment on column SETTLEMENT_HDR.CURRENCY_ISO_CODE is 'The cusrrency in international format (e.g., USD, GBP, etc.)';

comment on column SETTLEMENT_HDR.FILE_COMPLETE_FLG is 'This indicates whether the file was processed for all lines. 1 for complete, otherwise incomplete.';

comment on column SETTLEMENT_HDR.HEADER_TEXT is 'The full length Text of the file Header line.';

comment on column SETTLEMENT_HDR.TRAILER_TEXT is 'The full length Text of the file Trailer line.';

