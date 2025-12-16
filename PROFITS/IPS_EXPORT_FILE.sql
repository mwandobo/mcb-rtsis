create table IPS_EXPORT_FILE
(
    UNIQUE_ID            DECIMAL(15)  not null
        constraint PK_IPS_EXPORT_FILE
            primary key,
    FILENAME             CHAR(50)     not null,
    GROUP_NUMBER         DECIMAL(15),
    CLEARING_SYSTEM      CHAR(35)     not null,
    SETTLEMENT_BANK      CHAR(12),
    GROUP_TYPE           CHAR(3),
    SETTLEMENT_DATE      DATE,
    RECORD_TYPE          CHAR(20)     not null,
    PARENT_RECORD_TYPE   VARCHAR(20),
    ORDER_CODE           VARCHAR(20),
    FK_ORDER_CODE        VARCHAR(20),
    INSTRUMENT_JUSTIFIC  INTEGER,
    PARTICIPANT_BANK     CHAR(12)     not null,
    SETTLEMENT_AMOUNT    DECIMAL(15, 2),
    COUNTRY              CHAR(2),
    CUTOFF_TIME          SMALLINT,
    RECORD_LINE          VARCHAR(2048),
    TIMESTAMP            TIMESTAMP(6) not null,
    GROUP_MESSAGE_ID     VARCHAR(35),
    XML_SCHEMA           VARCHAR(10),
    ORI_GROUP_MESSAGE_ID VARCHAR(35)
);

create unique index SC_IPS_EXPORT_FILE
    on IPS_EXPORT_FILE (FILENAME, RECORD_TYPE, GROUP_NUMBER);

