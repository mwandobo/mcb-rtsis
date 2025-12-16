create table HOUSING_DETAILS
(
    PARTAIL_COMMENTS               CHAR(40),
    PARTIAL                        CHAR(1),
    ROOF                           INTEGER,
    WALE                           INTEGER,
    SKELETON                       INTEGER,
    NO_FLOORS                      SMALLINT,
    SQUARE                         SMALLINT,
    FLOOR                          SMALLINT,
    HOUSE_TYPE                     INTEGER,
    HOUSE_USE                      CHAR(1),
    ZIP_CODE                       CHAR(10),
    CITY                           CHAR(20),
    ADDRESS_2                      CHAR(40),
    ADDRESS_1                      CHAR(40),
    FK_ISS_COMMITMETP_SO_IDENTIFIE DECIMAL(10) not null
        constraint HOUSINGD
            primary key,
    USE_GROUND_FLOOR               CHAR(1),
    USE_BASEMENT                   CHAR(1),
    USE_FLOOR                      CHAR(1)     not null
);

