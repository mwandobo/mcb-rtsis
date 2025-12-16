create table VOC_DETAIL
(
    VOC_TYPE              CHAR(5),
    SERIAL_NUMBER         INTEGER,
    LANGUAGE              SMALLINT,
    DESCRIPTION           CHAR(254),
    FK_VOC_HEADERVOC_TYPE CHAR(5)
);

create unique index IXP_VOC_001
    on VOC_DETAIL (FK_VOC_HEADERVOC_TYPE, LANGUAGE, SERIAL_NUMBER);

