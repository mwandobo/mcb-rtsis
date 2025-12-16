create table ACC_RESTR_CHAN_TRX
(
    FK_CHANNEL_ID     INTEGER,
    FK_TRANS_ID       INTEGER,
    FK_ACCOUNT_NUMBER DECIMAL(11),
    STATUS_0          CHAR(1),
    STATUS_1          CHAR(1),
    STATUS_2          CHAR(1),
    STATUS_3          CHAR(1),
    STATUS_4          CHAR(1),
    STATUS_6          CHAR(1),
    STATUS_5          CHAR(1)
);

create unique index I0002906
    on ACC_RESTR_CHAN_TRX (FK_TRANS_ID, FK_CHANNEL_ID, FK_ACCOUNT_NUMBER);

