create table UNIT_NAME
(
    CODE                           SMALLINT     not null
        constraint PIX_UNIT
            primary key,
    TMSTAMP                        TIMESTAMP(6) not null,
    TRANSITION_FLAG                CHAR(1),
    ADDRESS                        VARCHAR(40)  not null,
    CITY                           VARCHAR(15)  not null,
    NODE                           VARCHAR(40),
    TELEPHONE_1                    VARCHAR(15),
    FB_POSITION_FLG                CHAR(1),
    ADDRESS_2                      CHAR(40),
    TELEPHONE_2                    CHAR(15),
    ZIP_CODE                       CHAR(10),
    CLEARING_HOUSE_FLAG            CHAR(1),
    HLD_WORK_PERMIT                CHAR(1),
    EMAIL                          CHAR(40),
    CURR_TRX_DATE                  DATE,
    ENTRY_STATUS                   CHAR(1),
    UNIT_NAME                      VARCHAR(40)  not null,
    FAX                            VARCHAR(15),
    FK_GENERIC_DETAFK_PARAMETER_TY CHAR(5),
    FK_GENERIC_DETASERIAL_NUM      INTEGER
);

