create table MSG_TRIGGER_LOG
(
    FK_MSG_ALERT_RURUL CHAR(5)  not null,
    TRX_DATE           DATE     not null,
    TRX_USR            CHAR(8)  not null,
    TRX_UNIT           SMALLINT not null,
    TRX_USR_SN         INTEGER  not null,
    TUN_INTERNAL_SN    SMALLINT not null,
    FK_DEP_TRX_RECOTUN SMALLINT,
    FK2DEP_TRX_RECOTRX INTEGER,
    FK_DEP_TRX_RECOTRX INTEGER,
    FK1DEP_TRX_RECOTRX DATE,
    FK0DEP_TRX_RECOTRX CHAR(8),
    constraint IXU_CIS_177
        primary key (FK_MSG_ALERT_RURUL, TRX_DATE, TRX_USR, TRX_UNIT, TRX_USR_SN, TUN_INTERNAL_SN)
);

