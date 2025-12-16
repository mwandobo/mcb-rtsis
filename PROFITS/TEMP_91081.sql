create table TEMP_91081
(
    HD_EXECUTION_DATE DATE not null,
    EXPIRY_DATE       DATE not null,
    constraint IXU_REP_173
        primary key (HD_EXECUTION_DATE, EXPIRY_DATE)
);

