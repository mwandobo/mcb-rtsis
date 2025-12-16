create table SO_SWIFT
(
    TP_SO_IDENTIFIER   DECIMAL(10),
    VALUE_DAYS         SMALLINT,
    ORIGIN_CODE        CHAR(2),
    SERVICE_CODE       CHAR(2),
    TRANS_CODE         CHAR(3),
    DETAIL_OF_CHARGE   CHAR(3),
    RECEIVER_DIAS_UNIT CHAR(4),
    CR_ACCOUNT_DTL     CHAR(34),
    INFORMATION6       CHAR(35),
    INFORMATION5       CHAR(35),
    JUSTIFIC1          CHAR(35),
    INFORMATION4       CHAR(35),
    INFORMATION3       CHAR(35),
    INFORMATION2       CHAR(35),
    INFORMATION1       CHAR(35),
    JUSTIFIC4          CHAR(35),
    JUSTIFIC3          CHAR(35),
    JUSTIFIC2          CHAR(35),
    IBAN               CHAR(37),
    FREE_TEXT          CHAR(154)
);

create unique index IXU_SO__004
    on SO_SWIFT (TP_SO_IDENTIFIER);

