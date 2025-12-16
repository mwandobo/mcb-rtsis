create table CMS_CAF_DISPATCHES
(
    EXPORT_ID DECIMAL(10) not null
        constraint PK_CMS_CAF_DISPATCHES
            primary key,
    CARD_SN   DECIMAL(10) not null,
    TRX_DATE  DATE,
    STATUS    CHAR(1),
    TMSTAMP   TIMESTAMP(6)
);

