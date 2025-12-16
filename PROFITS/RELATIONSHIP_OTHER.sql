create table RELATIONSHIP_OTHER
(
    OTHER_SN           DECIMAL(10) not null
        constraint PK_REL_OTHER
            primary key,
    LAST_NAME          VARCHAR(70),
    FIRST_NAME         VARCHAR(70),
    PERCENTAGE         DECIMAL(8, 4),
    MOBILE_PHONE       VARCHAR(40),
    HOME_PHONE         VARCHAR(40),
    WORK_PHONE         VARCHAR(40),
    RESIDENT_FLAG      CHAR(1),
    EMAIL              VARCHAR(200),
    ADDRESS_1          VARCHAR(40),
    ADDRESS_2          VARCHAR(40),
    ADDRESS_3          VARCHAR(40),
    ADDRESS_4          VARCHAR(40),
    REGION             VARCHAR(40),
    MAIL_BOX           VARCHAR(20),
    ZIP_CODE           VARCHAR(20),
    CITY               VARCHAR(40),
    COMMENTS           VARCHAR(500),
    CREATE_USER        CHAR(8),
    CREATE_UNIT        INTEGER,
    CREATE_DATE        DATE,
    CREATE_TMSTAMP     TIMESTAMP(6),
    UPDATE_USER        CHAR(8),
    UPDATE_UNIT        INTEGER,
    UPDATE_DATE        DATE,
    UPDATE_TMSTAMP     TIMESTAMP(6),
    ENTRY_STATUS       CHAR(1),
    FK_GH_CNTRY        CHAR(5),
    FK_GD_CNTRY        INTEGER,
    FK_GH_RERNG        CHAR(5),
    FK_GD_RERNG        INTEGER,
    FK_RELATIONSHIP_ID CHAR(12),
    FK_CUSTOMER_ID     INTEGER
);

