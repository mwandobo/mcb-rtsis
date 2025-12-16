create table SCANNED_IMAGE
(
    IMAGE_ID     DECIMAL(12) not null
        constraint IXU_SCA_001
            primary key,
    IMAGE_NUMBER INTEGER,
    IMAGE_LENGTH INTEGER,
    ACTIVE       CHAR(1),
    STORE_STATUS CHAR(1),
    USR_CODE     VARCHAR(20),
    IMAGE_TYPE   VARCHAR(20),
    OBJECT_ID    VARCHAR(50),
    IMAGE        BLOB(1048576)
);

create unique index IX01_SCANNED_IMAGE
    on SCANNED_IMAGE (OBJECT_ID);

