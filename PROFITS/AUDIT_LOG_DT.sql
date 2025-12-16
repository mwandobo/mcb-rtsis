create table AUDIT_LOG_DT
(
    TRX_DATE       DATE    not null,
    TRX_UNIT       INTEGER not null,
    TRX_USER       CHAR(8) not null,
    TRX_USR_SN     INTEGER not null,
    INTERNAL_SN    INTEGER not null,
    SERIAL_NUMBER  INTEGER not null,
    LITERAL        VARCHAR(200),
    PREVIOUS_VALUE VARCHAR(255),
    CURRENT_VALUE  VARCHAR(255),
    TMPSTAMP       TIMESTAMP(6),
    VIEW_NAME      VARCHAR(150)
);

create unique index PK_AUDIT_LOG_DT
    on AUDIT_LOG_DT (TRX_DATE, TRX_UNIT, TRX_USER, TRX_USR_SN, INTERNAL_SN, SERIAL_NUMBER);

