create table TP_DTL_INPUT_INTRF
(
    RECEIVED_DATE      DATE        not null,
    SERIAL_NUMBER      INTEGER     not null,
    INTERNAL_SN        INTEGER     not null,
    DTL_CR_DR_FLAG     CHAR(1),
    ACCOUNT_NO         DECIMAL(11),
    AMOUNT             DECIMAL(15, 2),
    DTL_STATUS         CHAR(1),
    ERROR_DESCRIPTION  VARCHAR(40),
    VALUE_DATE         DATE,
    TIMESTMP           TIMESTAMP(6),
    FK_TP_SO_COMMITMEN DECIMAL(10) not null,
    constraint PKTPINPU
        primary key (FK_TP_SO_COMMITMEN, RECEIVED_DATE, SERIAL_NUMBER, INTERNAL_SN)
);

