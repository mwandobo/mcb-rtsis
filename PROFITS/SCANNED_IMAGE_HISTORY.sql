create table SCANNED_IMAGE_HISTORY
(
    IMAGE_TIMESTAMP         TIMESTAMP(0) not null,
    IMAGE_ID                INTEGER      not null,
    IMAGE_NUMBER            INTEGER,
    IMAGE_LENGTH            INTEGER,
    ACTIVE                  CHAR(1),
    STORE_STATUS            CHAR(1),
    USR_CODE                VARCHAR(20),
    IMAGE_TYPE              VARCHAR(20),
    OBJECT_ID               VARCHAR(50),
    CHANGE_UNIT_CODE        INTEGER,
    CHANGE_TERMINAL_CODE    VARCHAR(16),
    CHANGE_USR_PROFILE_MAIN CHAR(8),
    CHANGE_USR_PROFILE_2    CHAR(8),
    CHANGE_USR_PROFILE_3    CHAR(8),
    constraint PK_SCANNED_IMAGE_HISTORY
        primary key (IMAGE_TIMESTAMP, IMAGE_ID)
);

