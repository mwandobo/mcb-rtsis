create table DHSSE_ERROR
(
    TMSTAMP    TIMESTAMP(6),
    ERROR_CODE CHAR(2),
    TRX_USER   CHAR(8),
    ERROR_DESC CHAR(80)
);

create unique index IXU_BILL_125
    on DHSSE_ERROR (ERROR_DESC, TMSTAMP);

